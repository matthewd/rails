# frozen_string_literal: true

require "cases/helper"
require "support/association_route_resolver"
require "models/comment"
require "models/post"

class AssociationRouteScopeTest < ActiveRecord::TestCase
  fixtures :posts

  def test_has_many_route_can_scope_its_destination
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      destination_scope: -> { where(body: "Routed body") }
    )
    post = posts(:welcome)
    matching = Comment.create!(post_id: post.id, body: "Routed body")
    mismatching = Comment.create!(post_id: post.id, body: "Other body")

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      assert_equal [matching], post.comments.where(id: [matching.id, mismatching.id]).to_a
      assert_equal "Routed body", post.comments.build.body

      preloaded = Post.where(id: post.id).preload(:comments).first
      assert_includes preloaded.comments, matching
      assert_not_includes preloaded.comments, mismatching

      assert Post.joins(:comments).where(posts: { id: post.id }, comments: { id: matching.id }).exists?
      assert_not Post.joins(:comments).where(posts: { id: post.id }, comments: { id: mismatching.id }).exists?
    end
  end

  def test_destination_scope_preserves_non_where_values
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      destination_scope: -> { order(:id).limit(1) }
    )
    post = Post.create!(title: "Limited route", body: "Limited route")
    first_comment = Comment.create!(post_id: post.id, body: "First")
    Comment.create!(post_id: post.id, body: "Second")

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      assert_equal [first_comment], post.comments.to_a
    end
  end

  def test_origin_dependent_route_scope_groups_preloads_and_cannot_join
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      destination_scope: ->(origin) { where(body: origin.title) }
    )
    first_post = Post.create!(title: "First routed body", body: "First post")
    second_post = Post.create!(title: "Second routed body", body: "Second post")
    first_comment = Comment.create!(post_id: first_post.id, body: first_post.title)
    second_comment = Comment.create!(post_id: second_post.id, body: second_post.title)
    Comment.create!(post_id: first_post.id, body: second_post.title)
    Comment.create!(post_id: second_post.id, body: first_post.title)

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      assert_equal [first_comment], first_post.comments.to_a
      assert_equal [second_comment], second_post.comments.to_a

      preloaded = Post.where(id: [first_post.id, second_post.id]).preload(:comments).index_by(&:id)
      assert_equal [first_comment], preloaded[first_post.id].comments
      assert_equal [second_comment], preloaded[second_post.id].comments

      assert_raises(ArgumentError, match: /instance-dependent association route/) do
        Post.joins(:comments).load
      end
    end
  end

  def test_preloading_mixed_scope_arities_keeps_routes_separate
    reflection = Post.reflect_on_association(:comments)
    origin_route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      destination_scope: ->(origin) { where(body: origin.title) }
    )
    static_route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      destination_scope: -> { where.not(body: nil) }
    )
    first_post = Post.create!(title: "First matching body", body: "First post")
    second_post = Post.create!(title: "Unused title", body: "Second post")
    first_comment = Comment.create!(post_id: first_post.id, body: first_post.title)
    second_comment = Comment.create!(post_id: second_post.id, body: "Second body")
    resolver = TestAssociationRouteResolver.new(
      route_for_target: ->(target) { target == first_post ? origin_route : static_route }
    )

    reflection.stub(:association_route_resolver, resolver) do
      preloaded = Post.where(id: [first_post.id, second_post.id]).order(:id).preload(:comments).index_by(&:id)

      assert_equal [first_comment], preloaded[first_post.id].comments
      assert_equal [second_comment], preloaded[second_post.id].comments
    end
  end

  def test_belongs_to_route_can_scope_its_destination
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "ScopedRoutedComment"

      belongs_to :routed_post, class_name: "Post", foreign_key: :post_id, optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    route = build_belongs_to_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      destination_scope: -> { where(title: "Routed title") }
    )
    matching = Post.create!(title: "Routed title", body: "Matching")
    mismatching = Post.create!(title: "Other title", body: "Mismatching")
    matching_reference = reference_class.create!(post_id: matching.id, body: "Matching reference")
    mismatching_reference = reference_class.create!(post_id: mismatching.id, body: "Mismatching reference")

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      assert_equal matching, matching_reference.routed_post
      assert_nil mismatching_reference.routed_post

      preloaded = reference_class.where(id: [matching_reference.id, mismatching_reference.id]).preload(:routed_post).index_by(&:id)
      assert_equal matching, preloaded[matching_reference.id].routed_post
      assert_nil preloaded[mismatching_reference.id].routed_post

      matching_join = reference_class.joins(:routed_post).where(id: matching_reference.id)
      mismatching_join = reference_class.joins(:routed_post).where(id: mismatching_reference.id)
      assert_predicate matching_join, :exists?, matching_join.to_sql
      assert_not_predicate mismatching_join, :exists?, mismatching_join.to_sql
    end
  end

  private
    def fixed_resolver(route)
      TestAssociationRouteResolver.new(route)
    end

    def build_belongs_to_route(reflection, reference:, destination_scope:)
      ActiveRecord::AssociationRoute.new(
        destination_class: reflection.klass,
        link: ActiveRecord::AssociationLink.new(
          reference: ActiveRecord::Key::Mapping.new(**reference)
        ),
        reference_on: :origin,
        destination_scope: destination_scope
      )
    end

    def build_route(reflection, reference:, destination_scope:)
      ActiveRecord::AssociationRoute.new(
        destination_class: reflection.klass,
        link: ActiveRecord::AssociationLink.new(
          reference: ActiveRecord::Key::Mapping.new(**reference)
        ),
        reference_on: :destination,
        destination_scope: destination_scope
      )
    end
end
