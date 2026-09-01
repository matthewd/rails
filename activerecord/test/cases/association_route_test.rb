# frozen_string_literal: true

require "cases/helper"
require "models/company"
require "models/member"
require "models/sponsor"
require "models/author"
require "models/person"
require "models/comment"
require "models/post"
require "models/item"
require "models/tagging"

class AssociationRouteTest < ActiveRecord::TestCase
  fixtures :authors, :comments, :items, :posts, :taggings

  def test_key_mapping_preserves_physical_column_pairs
    mapping = ActiveRecord::KeyMapping.new(
      referencing_key: [:account_id, :post_id],
      referenced_key: [:account_id, :id]
    )

    assert_equal [
      ["account_id", "account_id"],
      ["post_id", "id"],
    ], mapping.to_a
    assert_equal ["account_id", "post_id"], mapping.referencing_key.name
    assert_equal ["account_id", "id"], mapping.referenced_key.name
  end

  def test_key_mapping_requires_equal_arity
    error = assert_raises(ArgumentError) do
      ActiveRecord::KeyMapping.new(
        referencing_key: [:account_id, :post_id],
        referenced_key: :id
      )
    end

    assert_equal "association key mappings must have the same number of columns", error.message
  end

  def test_key_mapping_can_be_viewed_from_either_endpoint
    mapping = ActiveRecord::KeyMapping.new(
      referencing_key: [:account_id, :post_id],
      referenced_key: [:account_id, :id]
    )

    forward = mapping.from(:referencing)
    assert_equal mapping.referencing_key, forward.owner_key
    assert_equal mapping.referenced_key, forward.target_key
    assert_equal mapping.to_a, forward.to_a

    inverse = mapping.from(:referenced)
    assert_equal mapping.referenced_key, inverse.owner_key
    assert_equal mapping.referencing_key, inverse.target_key
    assert_equal mapping.to_a.map(&:reverse), inverse.to_a
  end

  def test_association_link_combines_constraints_with_the_writable_reference
    reference = ActiveRecord::KeyMapping.new(
      referencing_key: :post_id,
      referenced_key: :id
    )
    constraints = ActiveRecord::KeyMapping.new(
      referencing_key: :account_id,
      referenced_key: :account_id
    )
    link = ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints)

    assert_equal [["post_id", "id"]], link.reference.to_a
    assert_equal [["account_id", "account_id"]], link.constraints.to_a
    assert_equal [
      ["account_id", "account_id"],
      ["post_id", "id"],
    ], link.match.to_a
  end

  def test_belongs_to_route_is_physically_oriented_from_foreign_key
    reflection = Client.reflect_on_association(:firm)
    route = reflection.association_route

    assert_equal Client, route.referencing_class
    assert_equal Firm, route.referenced_class
    assert_equal :referencing, route.owner_side
    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "client_of", route.owner_key.name
    assert_equal "id", route.target_key.name
  end

  def test_has_many_route_preserves_physical_orientation_and_reverses_traversal
    reflection = Firm.reflect_on_association(:clients_of_firm)
    route = reflection.association_route

    assert_equal Client, route.referencing_class
    assert_equal Firm, route.referenced_class
    assert_equal :referenced, route.owner_side
    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "id", route.owner_key.name
    assert_equal "client_of", route.target_key.name
  end

  def test_route_writes_only_the_physical_referencing_record
    reflection = Client.reflect_on_association(:firm)
    route = reflection.association_route
    client = Client.new
    firm = Firm.new(id: 42)

    route.write(client, firm)

    assert_equal 42, client.client_of
    assert_equal 42, firm.id
  end

  def test_polymorphic_route_resolves_the_referenced_key_for_each_target_class
    reflection = PolymorphicComment.reflect_on_association(:person)
    author_route = reflection.association_route(Author)
    person_route = reflection.association_route(Person)

    assert_equal [["person_id", "author_code"]], author_route.link.reference.to_a
    assert_equal [["person_id", "external_id"]], person_route.link.reference.to_a
    assert_equal({ "person_type" => Author.polymorphic_name }, author_route.fixed_reference_values)
    assert_equal({ "person_type" => Person.polymorphic_name }, person_route.fixed_reference_values)
  end

  def test_inverse_polymorphic_route_has_the_same_physical_shape
    forward = Sponsor.reflect_on_association(:sponsorable).association_route(Member)
    inverse = Member.reflect_on_association(:sponsor).association_route

    assert_equal forward.referencing_class, inverse.referencing_class
    assert_equal forward.referenced_class, inverse.referenced_class
    assert_equal forward.link.reference, inverse.link.reference
    assert_equal :referencing, forward.owner_side
    assert_equal :referenced, inverse.owner_side
    assert_equal forward.fixed_reference_values, inverse.fixed_reference_values
  end

  def test_join_resolves_an_inverse_polymorphic_route_from_the_relation_model
    item = items(:dvd)
    tagging = taggings(:godfather)

    assert Item.joins(:tagging).where(items: { id: item.id }, taggings: { id: tagging.id }).exists?
  end

  def test_internal_query_constraints_apply_to_reads_but_not_writes
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reflection,
      reference: { referencing_key: :post_id, referenced_key: :id },
      constraints: { referencing_key: :body, referenced_key: :title }
    )
    post = posts(:welcome)
    matching = Comment.create!(post_id: post.id, body: post.title)
    mismatching = Comment.create!(post_id: post.id, body: "Not the post title")

    reflection.stub(:association_router, fixed_router(route)) do
      assert_equal [matching], post.comments.where(id: [matching.id, mismatching.id]).to_a

      built = post.comments.build
      assert_equal post.id, built.post_id
      assert_nil built.body
      assert_equal "Manual", post.comments.where(body: "Manual").build.body
      assert_equal "Explicit", post.comments.create_with(body: "Explicit").build.body

      preloaded = Post.where(id: post.id).preload(:comments).first
      assert_includes preloaded.comments, matching
      assert_not_includes preloaded.comments, mismatching

      assert Post.joins(:comments).where(posts: { id: post.id }, comments: { id: matching.id }).exists?
      assert_not Post.joins(:comments).where(posts: { id: post.id }, comments: { id: mismatching.id }).exists?
    end
  end

  def test_internal_routes_encapsulate_inverse_discriminator_filters
    member = Member.create!(name: "Routed member")
    sponsor = Sponsor.new(sponsorable_id: member.id, sponsorable_type: "routed")
    sponsor.save!(validate: false)

    forward_reflection = Sponsor.reflect_on_association(:sponsorable)
    forward_route = build_polymorphic_route(forward_reflection, Member, stored_type: "routed")
    inverse_reflection = Member.reflect_on_association(:sponsor)
    inverse_route = ActiveRecord::AssociationRoute.new(
      referencing_class: Sponsor,
      referenced_class: Member,
      link: forward_route.link,
      owner_side: :referenced,
      fixed_reference_values: forward_route.fixed_reference_values
    )

    forward_reflection.stub(:association_router, fixed_router(forward_route)) do
      inverse_reflection.stub(:association_router, fixed_router(inverse_route)) do
        assert_equal sponsor, member.sponsor

        preloaded = Member.where(id: member.id).preload(:sponsor).first
        assert_equal sponsor, preloaded.sponsor

        assert Member.joins(:sponsor).where(sponsors: { id: sponsor.id }).exists?
        assert_equal sponsor, Sponsor.where(sponsorable: member).first
      end
    end
  end

  def test_internal_router_can_reinterpret_a_discriminator_as_another_class
    first_target_class = Class.new(ActiveRecord::Base) do
      self.table_name = "companies"
      self.inheritance_column = nil

      def self.name = "FirstRoutedTarget"
    end
    second_target_class = Class.new(ActiveRecord::Base) do
      self.table_name = "accounts"

      def self.name = "SecondRoutedTarget"
    end
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "RoutedReference"

      belongs_to :routed_target,
        polymorphic: true,
        foreign_key: :sponsorable_id,
        foreign_type: :sponsorable_type,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_target)
    first_route = build_polymorphic_route(reflection, first_target_class, stored_type: "routed")
    second_route = build_polymorphic_route(reflection, second_target_class, stored_type: "routed")
    selected_route = first_route
    router = Object.new
    router.define_singleton_method(:route_for) { |*| selected_route }
    router.define_singleton_method(:route_for_referenced) { |*| selected_route }
    router.define_singleton_method(:resolve_reference) { |*| selected_route }
    router.define_singleton_method(:relation_route) { |**| selected_route }

    shared_id = 9_000_001
    first_target = first_target_class.create!(id: shared_id, name: "First target")
    second_target = second_target_class.create!(id: shared_id, credit_limit: 100)
    reference = reference_class.create!(sponsorable_id: shared_id, sponsorable_type: "routed")

    reflection.stub(:association_router, router) do
      assert_equal first_target, reference.routed_target

      selected_route = second_route
      reference = reference_class.find(reference.id)

      assert_equal second_target, reference.routed_target

      preloaded = reference_class.where(id: reference.id).preload(:routed_target).first
      assert_equal second_target, preloaded.routed_target

      replacement = second_target_class.create!(id: shared_id + 1, credit_limit: 200)
      reference.routed_target = replacement
      assert_equal replacement.id, reference.sponsorable_id
      assert_equal "routed", reference.sponsorable_type
    end
  end

  def test_internal_belongs_to_route_writes_and_clears_its_selected_key
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "RoutedComment"

      belongs_to :routed_post, class_name: "Post", foreign_key: :post_id, optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    id_route = build_belongs_to_route(
      reflection,
      reference: { referencing_key: :post_id, referenced_key: :id }
    )
    author_route = build_belongs_to_route(
      reflection,
      reference: { referencing_key: :author_id, referenced_key: :author_id }
    )
    selected_route = author_route
    router = Object.new
    router.define_singleton_method(:route_for) { |*| selected_route }
    router.define_singleton_method(:route_for_referenced) { |*| selected_route }
    router.define_singleton_method(:resolve_reference) { |*| selected_route }
    router.define_singleton_method(:relation_route) { |**| selected_route }
    post = Post.create!(author_id: 9_000_002, title: "Routed post", body: "Routed post body")
    comment = reference_class.new(post_id: -1, body: "Routed comment")

    reflection.stub(:association_router, router) do
      comment.routed_post = post

      assert_equal post.author_id, comment.author_id
      assert_equal(-1, comment.post_id)

      comment.routed_post = nil

      assert_nil comment.author_id
      assert_equal(-1, comment.post_id)

      comment.author_id = post.author_id
      comment.save!
      assert_equal post, comment.reload.routed_post

      selected_route = id_route
      comment = reference_class.find(comment.id)
      assert_nil comment.routed_post
    end
  end

  def test_internal_routes_can_switch_keys_between_the_same_models
    post_class = Class.new(ActiveRecord::Base) do
      self.table_name = "posts"
      self.inheritance_column = nil

      def self.name = "RoutedPost"

      has_many :routed_comments, class_name: "Comment", foreign_key: :post_id
    end
    reflection = post_class.reflect_on_association(:routed_comments)
    id_route = build_route(
      reflection,
      reference: { referencing_key: :post_id, referenced_key: :id }
    )
    author_route = build_route(
      reflection,
      reference: { referencing_key: :author_id, referenced_key: :author_id }
    )
    selected_route = id_route
    post = post_class.find(posts(:welcome).id)
    id_comment = Comment.create!(post_id: post.id, author_id: -1, body: "Matched by post id")
    author_comment = Comment.create!(post_id: -1, author_id: post.author_id, body: "Matched by author id")

    router = Object.new
    router.define_singleton_method(:route_for) { |*| selected_route }
    router.define_singleton_method(:route_for_referenced) { |*| selected_route }
    router.define_singleton_method(:relation_route) { |**| selected_route }

    reflection.stub(:association_router, router) do
      assert_equal [id_comment], post.routed_comments.where(id: [id_comment.id, author_comment.id]).to_a
      assert_equal post.id, post.routed_comments.build.post_id

      selected_route = author_route
      post = post_class.find(post.id)

      assert_equal [author_comment], post.routed_comments.where(id: [id_comment.id, author_comment.id]).to_a
      built = post.routed_comments.build
      assert_equal post.author_id, built.author_id
      assert_nil built.post_id

      preloaded = post_class.where(id: post.id).preload(:routed_comments).first
      assert_includes preloaded.routed_comments, author_comment
      assert_not_includes preloaded.routed_comments, id_comment

      assert post_class.joins(:routed_comments).where(comments: { id: author_comment.id }).exists?
      assert_not post_class.joins(:routed_comments).where(comments: { id: id_comment.id }).exists?
    end
  end

  private
    def fixed_router(route)
      Object.new.tap do |router|
        router.define_singleton_method(:route_for) { |*| route }
        router.define_singleton_method(:route_for_referenced) { |*| route }
        router.define_singleton_method(:resolve_reference) { |*| route }
        router.define_singleton_method(:relation_route) { |**| route }
      end
    end

    def build_belongs_to_route(reflection, reference:, constraints: nil)
      reference = ActiveRecord::KeyMapping.new(**reference)
      constraints = ActiveRecord::KeyMapping.new(**constraints) if constraints

      ActiveRecord::AssociationRoute.new(
        referencing_class: reflection.active_record,
        referenced_class: reflection.klass,
        link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints || ActiveRecord::KeyMapping.empty),
        owner_side: :referencing
      )
    end

    def build_polymorphic_route(reflection, target_class, stored_type:)
      ActiveRecord::AssociationRoute.new(
        referencing_class: reflection.active_record,
        referenced_class: target_class,
        link: ActiveRecord::AssociationLink.new(
          reference: ActiveRecord::KeyMapping.new(
            referencing_key: reflection.foreign_key,
            referenced_key: target_class.primary_key
          )
        ),
        owner_side: :referencing,
        fixed_reference_values: { reflection.foreign_type => stored_type }
      )
    end

    def build_route(reflection, reference:, constraints: nil)
      reference = ActiveRecord::KeyMapping.new(**reference)
      constraints = ActiveRecord::KeyMapping.new(**constraints) if constraints

      ActiveRecord::AssociationRoute.new(
        referencing_class: reflection.klass,
        referenced_class: reflection.active_record,
        link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints || ActiveRecord::KeyMapping.empty),
        owner_side: :referenced
      )
    end
end
