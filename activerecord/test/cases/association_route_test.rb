# frozen_string_literal: true

require "cases/helper"
require "models/company"
require "models/member"
require "models/sponsor"
require "models/author"
require "models/person"
require "models/comment"
require "models/item"
require "models/tagging"
require "models/tag"
require "models/post"
require "models/ship"

class AssociationRouteTest < ActiveRecord::TestCase
  fixtures :items, :posts, :taggings

  class ThroughAutosaveDestination < ActiveRecord::Base
    self.table_name = "companies"
    self.inheritance_column = nil
    self.primary_key = :firm_id
  end

  class RoutedItem < Item
    has_one :routed_item,
      through: :tagging,
      source: :taggable,
      source_type: "Item"
  end

  class AliasedThroughRouteLink < ActiveRecord::Base
    self.table_name = "sponsors"

    alias_attribute :route_fk, :club_id
    belongs_to :ship,
      class_name: "Ship",
      foreign_key: :route_fk,
      primary_key: :pirate_id,
      optional: true
  end

  class RemappedThroughRouteLink < AliasedThroughRouteLink
    alias_attribute :route_fk, :sponsor_id
  end

  class AliasedThroughRoutePost < ActiveRecord::Base
    self.table_name = "posts"
    self.inheritance_column = nil

    has_many :aliased_route_links,
      class_name: "AssociationRouteTest::RemappedThroughRouteLink",
      foreign_key: :sponsorable_id
    has_many :aliased_route_ships,
      through: :aliased_route_links,
      source: :ship
  end

  class InverseThroughRouteComment < ActiveRecord::Base
    self.table_name = "comments"
    self.inheritance_column = nil

    alias_attribute :route_fk, :post_id
  end

  class InverseThroughRoutePost < ActiveRecord::Base
    self.table_name = "posts"
    self.inheritance_column = nil

    alias_attribute :route_fk, :author_id
    has_many :inverse_route_comments,
      class_name: "AssociationRouteTest::InverseThroughRouteComment",
      foreign_key: :route_fk
  end

  class InverseThroughRouteAuthor < ActiveRecord::Base
    self.table_name = "authors"

    has_many :inverse_route_posts,
      class_name: "AssociationRouteTest::InverseThroughRoutePost",
      foreign_key: :author_id
    has_many :inverse_route_comments,
      through: :inverse_route_posts,
      source: :inverse_route_comments
  end

  def test_belongs_to_route_is_physically_oriented_from_foreign_key
    reflection = Client.reflect_on_association(:firm)
    route = reflection.association_route

    assert_not_predicate route, :reference_on_destination?
    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "client_of", route.origin_key.name
    assert_equal "id", route.destination_key.name
  end

  def test_has_many_route_preserves_physical_orientation_and_reverses_traversal
    reflection = Firm.reflect_on_association(:clients_of_firm)
    route = reflection.association_route

    assert_predicate route, :reference_on_destination?
    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "id", route.origin_key.name
    assert_equal "client_of", route.destination_key.name
  end

  def test_route_writes_only_the_physical_reference_record
    reflection = Client.reflect_on_association(:firm)
    route = reflection.association_route
    client = Client.new
    firm = Firm.new(id: 42)

    route.write(client, firm)

    assert_equal 42, client.client_of
    assert_equal 42, firm.id
  end

  def test_belongs_to_build_applies_scope_values_for_its_reference_key
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "ScopedReferenceKeyComment"

      belongs_to :routed_parent,
        -> { where(post_id: 42) },
        class_name: "Comment",
        foreign_key: :post_id,
        optional: true
    end
    reference = reference_class.new

    built = reference.association(:routed_parent).build(post_id: 7)

    assert_equal 42, built.post_id
  end

  def test_belongs_to_assignment_uses_the_assigned_subclass_key
    target_class = Class.new(Firm) do
      self.primary_key = :firm_id
    end
    firm = target_class.new(firm_id: 42)
    client = Client.new

    client.firm = firm

    assert_equal 42, client.client_of

    client.name = "Subclass key client"
    client.save!(validate: false)
    assert_equal client, Client.where(firm: firm).first
    assert_equal client, Client.find_by(firm: firm)
  end

  def test_equivalent_belongs_to_routes_group_sti_values
    post = Post.new(id: 1)
    special_post = SpecialPost.new(id: 2)

    assert_equal Comment.where(post_id: [post.id, special_post.id]).to_sql,
      Comment.where(post: [post, special_post]).to_sql
  end

  def test_belongs_to_route_uses_the_concrete_reference_class_aliases
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "AliasedRouteComment"

      alias_attribute :route_id, :post_id
      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :route_id,
        optional: true,
        inverse_of: false
    end
    subclass = Class.new(reference_class) do
      def self.name = "RemappedRouteComment"

      alias_attribute :route_id, :author_id
    end
    unrelated = Post.create!(title: "Unrelated", body: "Unrelated")
    target = Post.create!(title: "Target", body: "Target")
    replacement = Post.create!(title: "Replacement", body: "Replacement")
    reference = subclass.new(post_id: unrelated.id, author_id: target.id, body: "Reference")

    assert_equal target, reference.routed_post

    reference.routed_post = replacement
    reference.save!

    assert_equal unrelated.id, reference.post_id
    assert_equal replacement.id, reference.author_id
    assert_equal reference, subclass.find_by(routed_post: replacement)
    assert_equal [reference], subclass.where(routed_post: replacement).to_a
    assert_equal [reference.id], subclass.joins(:routed_post).where(posts: { id: replacement.id }).pluck(:id)
    assert_equal replacement, subclass.eager_load(:routed_post).find(reference.id).routed_post

    base = reference_class.create!(post_id: target.id, author_id: unrelated.id, body: "Base reference")
    reference.association(:routed_post).reset
    ActiveRecord::Associations::Preloader.new(records: [base, reference], associations: :routed_post).call

    assert_equal target, base.routed_post
    assert_equal replacement, reference.routed_post
  end

  def test_inverse_route_uses_the_concrete_origin_class_aliases
    origin_class = Class.new(ActiveRecord::Base) do
      self.table_name = "posts"
      self.inheritance_column = nil

      def self.name = "AliasedRoutePost"

      alias_attribute :route_key, :id
      has_many :routed_comments,
        class_name: "Comment",
        foreign_key: :post_id,
        primary_key: :route_key,
        inverse_of: false
    end
    subclass = Class.new(origin_class) do
      def self.name = "RemappedRoutePost"

      alias_attribute :route_key, :author_id
    end
    owner = subclass.new(id: 10, author_id: 20)

    assert_equal 20, owner.routed_comments.build.post_id
  end

  def test_polymorphic_route_resolves_the_target_key_for_each_destination_class
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

    assert_equal forward.link.reference, inverse.link.reference
    assert_not_predicate forward, :reference_on_destination?
    assert_predicate inverse, :reference_on_destination?
    assert_equal forward.fixed_reference_values, inverse.fixed_reference_values
  end

  def test_join_resolves_an_inverse_polymorphic_route_from_the_relation_model
    item = items(:dvd)
    tagging = taggings(:godfather)

    assert Item.joins(:tagging).where(items: { id: item.id }, taggings: { id: tagging.id }).exists?
  end

  def test_nested_through_route_uses_the_adjacent_class_for_an_inherited_polymorphic_inverse
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "NestedRoutedReference"

      belongs_to :routed_item, class_name: "Item", foreign_key: :sponsorable_id
      has_one :routed_tagging, through: :routed_item, source: :tagging
    end
    item = items(:dvd)
    reference = reference_class.create!(sponsorable_id: item.id)

    assert_equal taggings(:godfather), reference.routed_tagging
  end

  def test_polymorphic_through_route_uses_the_concrete_origin_class
    item = RoutedItem.find(items(:dvd).id)

    assert_equal item.id, item.routed_item.id
  end

  def test_through_route_uses_the_concrete_intermediate_class_aliases
    owner = AliasedThroughRoutePost.create!(title: "Aliased through", body: "Aliased through")
    ship = Ship.create!(pirate_id: 9_000_411, name: "Aliased through ship")
    RemappedThroughRouteLink.create!(
      sponsorable_id: owner.id,
      club_id: -1,
      sponsor_id: ship.pirate_id
    )

    assert_equal [ship], owner.aliased_route_ships.to_a
    assert_equal [ship], AliasedThroughRoutePost.where(id: owner.id).preload(:aliased_route_ships).first.aliased_route_ships
    assert_equal [owner.id], AliasedThroughRoutePost.joins(:aliased_route_ships).where(ships: { id: ship.id }).pluck(:id)
  end

  def test_inverse_through_route_uses_the_destination_class_aliases
    owner = InverseThroughRouteAuthor.create!(name: "Inverse through")
    post = InverseThroughRoutePost.create!(author_id: owner.id, title: "Inverse through", body: "Inverse through")
    comment = InverseThroughRouteComment.create!(post_id: post.id, author_id: -1, body: "Inverse through comment")

    assert_equal [comment], owner.inverse_route_comments.to_a
    assert_equal [comment], InverseThroughRouteAuthor.where(id: owner.id).preload(:inverse_route_comments).first.inverse_route_comments
    assert_equal [owner.id], InverseThroughRouteAuthor.joins(:inverse_route_comments).where(comments: { id: comment.id }).pluck(:id)
  end

  def test_polymorphic_association_class_tracks_direct_type_changes
    sponsor = Sponsor.new(sponsorable_type: Member.polymorphic_name)
    association = sponsor.association(:sponsorable)

    assert_equal Member, association.klass

    sponsor.sponsorable_type = Firm.polymorphic_name

    assert_equal Company, association.klass
  end

  def test_polymorphic_association_class_uses_the_stored_sti_type_after_assignment
    sponsor = Sponsor.new
    sponsor.sponsorable = Firm.new

    assert_equal Company.polymorphic_name, sponsor.sponsorable_type
    assert_equal Company, sponsor.association(:sponsorable).klass
  end

  def test_polymorphic_assignment_ignores_an_unresolvable_stored_type
    member = Member.create!(name: "Replacement target")
    sponsor = Sponsor.new(sponsorable_id: -1, sponsorable_type: "MissingRoutedClass")

    assert_nothing_raised { sponsor.sponsorable = member }
    assert_same member, sponsor.sponsorable
    assert_equal member.id, sponsor.sponsorable_id
    assert_equal Member.polymorphic_name, sponsor.sponsorable_type
  end

  def test_empty_polymorphic_through_collection_can_be_cleared
    tag = Tag.create!(id: 9_000_001, name: "Unused route")

    assert_empty tag.tagged_posts
    assert_nothing_raised { tag.tagged_posts.clear }
  end

  def test_has_one_through_autosave_does_not_route_from_the_outer_owner
    origin_class = Class.new(ActiveRecord::Base) do
      self.table_name = "member_details"
      self.primary_key = [:organization_id, :member_id]

      def self.name = "CompositeThroughRouteOrigin"

      belongs_to :member
      has_one :routed_destination,
        through: :member,
        source: :admittable,
        source_type: "AssociationRouteTest::ThroughAutosaveDestination"
    end
    destination = ThroughAutosaveDestination.create!(firm_id: 42, name: "Through route")
    member = Member.create!(admittable: destination)
    origin = origin_class.create!(organization_id: 9_000_001, member_id: member.id, extra_data: "Before")
    assert_equal destination, origin.routed_destination

    origin.extra_data = "After"

    assert_nothing_raised { origin.save! }
  end

  def test_historical_polymorphic_route_resolves_an_aliased_type
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"

      def self.name = "AliasedTypeRoutedReference"

      alias_attribute :routed_type, :author_type
      belongs_to :routed_target,
        polymorphic: true,
        foreign_key: :author_id,
        foreign_type: :routed_type,
        counter_cache: :legacy_comments_count,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_target)
    route = reflection.association_route(Post)
    assert_equal({ "author_type" => Post.polymorphic_name }, route.fixed_reference_values)

    target = Post.create!(title: "Aliased route", body: "Aliased route")
    target.update_column(:legacy_comments_count, 1)
    reference_class.insert_all!([
      {
        post_id: target.id,
        body: "Aliased route reference",
        author_id: target.id,
        author_type: Post.polymorphic_name,
      }
    ])
    reference = reference_class.find_by!(body: "Aliased route reference")

    reference.author_id = nil
    reference.save!

    assert_equal 0, target.reload.legacy_comments_count
  end

  def test_touch_does_not_resolve_unchanged_missing_polymorphic_reference
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "TouchRoutedReference"

      belongs_to :routed_target,
        polymorphic: true,
        foreign_key: :sponsorable_id,
        foreign_type: :sponsorable_type,
        touch: true,
        optional: true
    end
    target = Member.create!(name: "Touch route target")
    reference_class.insert_all!([
      { sponsorable_id: target.id, sponsorable_type: "MissingRoutedClass" }
    ])
    reference = reference_class.find_by!(sponsorable_type: "MissingRoutedClass")

    reference.sponsorable_type = Member.polymorphic_name

    assert_nothing_raised { reference.save! }

    reference_class.insert_all!([
      { club_id: 2, sponsorable_id: nil, sponsorable_type: "MissingRoutedClass" }
    ])
    reference = reference_class.find_by!(club_id: 2)
    assert_nothing_raised { reference.update!(routed_target: target) }
  end

  def test_partial_composite_reference_does_not_decrement_counters
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "CompositeCounterReference"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: [:club_id, :sponsorable_id],
        primary_key: [:author_id, :id],
        counter_cache: :legacy_comments_count,
        optional: true
    end
    post = Post.create!(author_id: 9_000_001, title: "Composite counter", body: "Composite counter")
    reference = reference_class.create!(club_id: 9_000_000, sponsorable_id: nil)
    association = reference.association(:routed_post)
    counter_changes = []

    association.stub(:update_counters_via_scope, ->(_klass, _values, by, _route) { counter_changes << by }) do
      reference.update!(club_id: post.author_id, sponsorable_id: post.id)
    end

    assert_equal [1], counter_changes
  end

  def test_empty_association_array_predicate_is_false
    assert_empty Client.where(firm: [])
  end

  def test_reference_metadata_does_not_resolve_a_missing_polymorphic_class
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "MissingClassRoutedReference"

      belongs_to :routed_target,
        polymorphic: true,
        foreign_key: :sponsorable_id,
        foreign_type: :sponsorable_type,
        counter_cache: true,
        optional: true
    end
    reference_class.insert_all!([
      { club_id: 1, sponsorable_id: nil, sponsorable_type: "MissingNamespace::MissingRoutedClass" }
    ])
    reference = reference_class.find_by!(club_id: 1)
    association = reference.association(:routed_target)

    reference_class.stub(:polymorphic_class_for, ->(*) { flunk("foreign-key metadata resolved the polymorphic class") }) do
      assert_equal ActiveRecord::Key.for(:sponsorable_id), association.foreign_key
      assert_not_predicate association, :target_changed?
    end

    reference.club_id = 2
    assert_nothing_raised { reference.save! }
  end

  def test_polymorphic_reference_can_be_cleared_when_stored_class_is_missing
    sponsor = Sponsor.new(sponsorable_id: 42, sponsorable_type: "MissingNamespace::MissingRoutedClass")

    assert_nothing_raised { sponsor.sponsorable = nil }
    assert_nil sponsor.sponsorable_id
    assert_nil sponsor.sponsorable_type
  end
end
