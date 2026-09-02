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

  def test_association_link_contains_the_physical_reference
    reference = ActiveRecord::Key::Mapping.new(
      reference_key: :post_id,
      target_key: :id
    )
    link = ActiveRecord::AssociationLink.new(reference: reference)

    assert_equal [["post_id", "id"]], link.reference.to_a

    equivalent_link = ActiveRecord::AssociationLink.new(reference: reference)
    assert_equal link, equivalent_link
    assert_equal link.hash, equivalent_link.hash
  end

  def test_belongs_to_route_is_physically_oriented_from_foreign_key
    reflection = Client.reflect_on_association(:firm)
    route = reflection.association_route

    assert_equal Firm, route.destination_class
    assert_not_predicate route, :reference_on_destination?
    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "client_of", route.origin_key.name
    assert_equal "id", route.destination_key.name
  end

  def test_has_many_route_preserves_physical_orientation_and_reverses_traversal
    reflection = Firm.reflect_on_association(:clients_of_firm)
    route = reflection.association_route

    assert_equal Client, route.destination_class
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

  def test_inverse_matching_uses_only_the_selected_route_direction
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "companies"
      self.inheritance_column = nil

      def self.name = "DirectionalRouteCompany"

      belongs_to :routed_company,
        class_name: "Company",
        foreign_key: :firm_id,
        optional: true
    end
    reference = reference_class.new(id: 1, firm_id: 99)
    destination = Company.new(id: 2, firm_id: reference.id)
    association = reference.association(:routed_company)

    assert_not association.send(:matches_foreign_key?, destination)
  end

  def test_belongs_to_build_does_not_apply_scope_values_for_its_reference_key
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

    assert_equal Member, forward.destination_class
    assert_equal Sponsor, inverse.destination_class
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

  def test_polymorphic_association_class_tracks_direct_type_changes
    sponsor = Sponsor.new(sponsorable_type: Member.polymorphic_name)
    association = sponsor.association(:sponsorable)

    assert_equal Member, association.klass

    sponsor.sponsorable_type = Firm.polymorphic_name

    assert_equal Company, association.klass
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

    assert_equal ActiveRecord::Key.for(:sponsorable_id), association.foreign_key
    assert_not_predicate association, :target_changed?

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
