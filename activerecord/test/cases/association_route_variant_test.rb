# frozen_string_literal: true

require "cases/helper"
require "support/association_route_resolver"
require "models/company"
require "models/member"
require "models/sponsor"
require "models/author"
require "models/comment"
require "models/post"
require "models/ship"

class AssociationRouteVariantTest < ActiveRecord::TestCase
  fixtures :posts

  class EagerDestination < ActiveRecord::Base
    self.table_name = "speedometers"
    self.primary_key = nil
  end

  class EagerOrigin < ActiveRecord::Base
    self.table_name = "dashboards"
    self.primary_key = :dashboard_id

    has_one :routed_destination,
      class_name: "AssociationRouteVariantTest::EagerDestination",
      foreign_key: :dashboard_id,
      primary_key: :dashboard_id
  end

  def test_discriminator_can_select_destination_class_and_reference_mapping
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "DiscriminatorRoutedReference"

      belongs_to :routed_target,
        polymorphic: true,
        foreign_key: :sponsorable_id,
        foreign_type: :sponsorable_type,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_target)
    member_route = build_polymorphic_route(
      reflection,
      Member,
      stored_type: "member",
      reference_key: :sponsorable_id
    )
    firm_route = build_polymorphic_route(
      reflection,
      Firm,
      stored_type: "firm",
      reference_key: :club_id
    )
    routes_by_type = {
      "member" => member_route,
      "firm" => firm_route,
    }
    routes_by_class = {
      Member => member_route,
      Firm => firm_route,
    }
    resolver = TestAssociationRouteResolver.new(
      route_for: ->(destination) {
        routes_by_class.fetch(destination.is_a?(Class) ? destination : destination.class)
      },
      route_for_target: ->(target) { routes_by_class.fetch(target.class) },
      resolve_reference: ->(reference_record, reader) {
        reader ||= ->(column) { reference_record.read_attribute(column) }
        routes_by_type[reader.call(reflection.foreign_type)]
      },
      relation_route: ->(_) { nil }
    )

    member = Member.create!(name: "Type-routed member")
    firm = Firm.create!(name: "Type-routed firm")
    member_reference = reference_class.create!(
      sponsorable_id: member.id,
      club_id: -1,
      sponsorable_type: "member"
    )
    firm_reference = reference_class.create!(
      sponsorable_id: -1,
      club_id: firm.id,
      sponsorable_type: "firm"
    )

    reflection.stub(:association_route_resolver, resolver) do
      assert_equal member, member_reference.routed_target
      assert_equal firm, firm_reference.routed_target

      preloaded = reference_class
        .where(id: [member_reference.id, firm_reference.id])
        .preload(:routed_target)
        .index_by(&:id)
      assert_equal member, preloaded[member_reference.id].routed_target
      assert_equal firm, preloaded[firm_reference.id].routed_target

      replacement = Firm.create!(name: "Replacement type-routed firm")
      member_reference.routed_target = replacement
      assert_equal replacement.id, member_reference.club_id
      assert_equal member.id, member_reference.sponsorable_id
      assert_equal "firm", member_reference.sponsorable_type

      member_reference.save!
      member_reference.reload.routed_target = nil
      assert_nil member_reference.club_id
      assert_equal member.id, member_reference.sponsorable_id
      assert_nil member_reference.sponsorable_type
    end
  end

  def test_compatible_independently_resolved_routes_establish_an_inverse
    member = Member.create!(name: "Inverse-routed member")
    sponsor = Sponsor.new(sponsorable_id: member.id, sponsorable_type: "routed")
    sponsor.save!(validate: false)
    forward_reflection = Sponsor.reflect_on_association(:sponsorable)
    forward_route = build_polymorphic_route(forward_reflection, Member, stored_type: "routed")
    inverse_reflection = Member.reflect_on_association(:sponsor)
    inverse_route = ActiveRecord::AssociationRoute.new(
      destination_class: Sponsor,
      link: forward_route.link,
      reference_on: :destination,
      fixed_reference_values: forward_route.fixed_reference_values
    )

    forward_reflection.stub(:association_route_resolver, TestAssociationRouteResolver.new(forward_route)) do
      inverse_reflection.stub(:association_route_resolver, TestAssociationRouteResolver.new(inverse_route)) do
        loaded_sponsor = member.sponsor

        assert_predicate loaded_sponsor.association(:sponsorable), :loaded?
        assert_same member, loaded_sponsor.sponsorable
      end
    end
  end

  def test_touch_uses_the_selected_historical_reference
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "TouchVariantReference"

      belongs_to :routed_ship,
        class_name: "Ship",
        foreign_key: :sponsorable_id,
        touch: true,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_ship)
    route = ActiveRecord::AssociationRoute.new(
      destination_class: Ship,
      link: ActiveRecord::AssociationLink.new(
        reference: ActiveRecord::Key::Mapping.new(
          reference_key: :club_id,
          target_key: :pirate_id
        )
      ),
      reference_on: :origin
    )
    resolver = TestAssociationRouteResolver.new(route)
    original_time = Time.utc(2000)
    old_ship = Ship.create!(name: "Old routed ship", pirate_id: 9_000_011, updated_at: original_time)
    new_ship = Ship.create!(name: "New routed ship", pirate_id: 9_000_012, updated_at: original_time)
    reference_class.insert_all!([
      { club_id: old_ship.pirate_id, sponsorable_id: -1 }
    ])
    reference = reference_class.find_by!(club_id: old_ship.pirate_id)

    reflection.stub(:association_route_resolver, resolver) do
      reference.club_id = new_ship.pirate_id
      reference.save!
    end

    assert_operator old_ship.reload.updated_at, :>, original_time
  end

  def test_routes_can_switch_keys_between_the_same_models
    post_class = Class.new(ActiveRecord::Base) do
      self.table_name = "posts"
      self.inheritance_column = nil

      def self.name = "RoutedPost"

      has_many :routed_comments, class_name: "Comment", foreign_key: :post_id
    end
    reflection = post_class.reflect_on_association(:routed_comments)
    id_route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id }
    )
    author_route = build_route(
      reflection,
      reference: { reference_key: :author_id, target_key: :author_id }
    )
    selected_route = id_route
    post = post_class.find(posts(:welcome).id)
    id_comment = Comment.create!(post_id: post.id, author_id: -1, body: "Matched by post id")
    author_comment = Comment.create!(post_id: -1, author_id: post.author_id, body: "Matched by author id")

    resolver = TestAssociationRouteResolver.new(
      route_for: ->(_) { selected_route },
      route_for_target: ->(_) { selected_route },
      relation_route: ->(_) { selected_route }
    )

    reflection.stub(:association_route_resolver, resolver) do
      assert_equal [id_comment], post.routed_comments.where(id: [id_comment.id, author_comment.id]).to_a
      assert_equal post.id, post.routed_comments.build.post_id

      selected_route = author_route
      post = post_class.find(post.id)

      assert_equal [author_comment], post.routed_comments.where(id: [id_comment.id, author_comment.id]).to_a
      built = post.routed_comments.build(author_id: -1)
      assert_equal post.author_id, built.author_id
      assert_nil built.post_id

      preloaded = post_class.where(id: post.id).preload(:routed_comments).first
      assert_includes preloaded.routed_comments, author_comment
      assert_not_includes preloaded.routed_comments, id_comment

      assert post_class.joins(:routed_comments).where(comments: { id: author_comment.id }).exists?
      assert_not post_class.joins(:routed_comments).where(comments: { id: id_comment.id }).exists?
    end
  end

  def test_eager_loading_uses_the_selected_destination_key
    reflection = EagerOrigin.reflect_on_association(:routed_destination)
    route = ActiveRecord::AssociationRoute.new(
      destination_class: EagerDestination,
      link: ActiveRecord::AssociationLink.new(
        reference: ActiveRecord::Key::Mapping.new(
          reference_key: :speedometer_id,
          target_key: :dashboard_id
        )
      ),
      reference_on: :destination
    )
    origin = EagerOrigin.create!(dashboard_id: "routed")
    EagerDestination.create!(speedometer_id: origin.id, name: "Routed destination")

    reflection.stub(:association_route_resolver, TestAssociationRouteResolver.new(route)) do
      loaded = EagerOrigin.eager_load(:routed_destination).find(origin.id)

      assert_equal "Routed destination", loaded.routed_destination.name
    end
  end

  def test_route_changes_are_part_of_through_statement_cache_shape
    author = Author.create!(name: "Routed author")
    post = Post.create!(author_id: author.id, title: "Routed through post", body: "Routed through post")
    id_comment = Comment.create!(post_id: post.id, author_id: -1, body: "Matched by post id")
    author_comment = Comment.create!(post_id: -1, author_id: author.id, body: "Matched by author id")
    source_reflection = Post.reflect_on_association(:comments)
    id_route = build_route(
      source_reflection,
      reference: { reference_key: :post_id, target_key: :id }
    )
    author_route = build_route(
      source_reflection,
      reference: { reference_key: :author_id, target_key: :author_id }
    )
    selected_route = id_route
    resolver = TestAssociationRouteResolver.new(
      route_for: ->(_) { selected_route },
      route_for_target: ->(_) { selected_route },
      relation_route: ->(_) { selected_route }
    )

    source_reflection.stub(:association_route_resolver, resolver) do
      assert_includes author.comments, id_comment
      assert_not_includes author.comments, author_comment

      selected_route = author_route
      author = Author.find(author.id)

      assert_includes author.comments, author_comment
      assert_not_includes author.comments, id_comment
    end
  end

  private
    def build_polymorphic_route(reflection, destination_class, stored_type:, reference_key: reflection.foreign_key)
      ActiveRecord::AssociationRoute.new(
        destination_class: destination_class,
        link: ActiveRecord::AssociationLink.new(
          reference: ActiveRecord::Key::Mapping.new(
            reference_key: reference_key,
            target_key: destination_class.primary_key
          )
        ),
        reference_on: :origin,
        fixed_reference_values: { reflection.foreign_type => stored_type }
      )
    end

    def build_route(reflection, reference:)
      ActiveRecord::AssociationRoute.new(
        destination_class: reflection.klass,
        link: ActiveRecord::AssociationLink.new(
          reference: ActiveRecord::Key::Mapping.new(**reference)
        ),
        reference_on: :destination
      )
    end
end
