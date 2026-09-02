# frozen_string_literal: true

require "cases/helper"
require "support/association_route_resolver"
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

  def test_association_link_combines_constraints_with_the_reference
    reference = ActiveRecord::Key::Mapping.new(
      reference_key: :post_id,
      target_key: :id
    )
    constraints = ActiveRecord::Key::Mapping.new(
      reference_key: :account_id,
      target_key: :account_id
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

  def test_internal_query_constraints_apply_to_reads_but_not_writes
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reflection,
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :body, target_key: :title }
    )
    post = posts(:welcome)
    matching = Comment.create!(post_id: post.id, body: post.title)
    mismatching = Comment.create!(post_id: post.id, body: "Not the post title")

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
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

  def test_query_constraints_identify_counter_cache_destinations
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "ConstrainedCounterReference"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :post_id,
        counter_cache: :legacy_comments_count,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    route = build_belongs_to_route(
      reflection,
      reference: { reference_key: :author_id, target_key: :author_id },
      constraints: { reference_key: :body, target_key: :title }
    )
    old_post = Post.create!(author_id: 9_000_101, title: "Old constrained post", body: "Old")
    decoy = Post.create!(author_id: old_post.author_id, title: "Decoy constrained post", body: "Decoy")
    new_post = Post.create!(author_id: 9_000_102, title: "New constrained post", body: "New")
    old_post.update_column(:legacy_comments_count, 1)
    decoy.update_column(:legacy_comments_count, 1)
    reference_class.insert_all!([
      {
        post_id: -1,
        author_id: old_post.author_id,
        body: old_post.title,
      }
    ])
    reference = reference_class.find_by!(body: old_post.title)

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      reference.author_id = new_post.author_id
      reference.body = new_post.title
      reference.save!
    end

    assert_equal 0, old_post.reload.legacy_comments_count
    assert_equal 1, decoy.reload.legacy_comments_count
    assert_equal 1, new_post.reload.legacy_comments_count
  end

  def test_query_constraints_identify_touch_destinations
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "ConstrainedTouchReference"

      belongs_to :routed_ship,
        class_name: "Ship",
        foreign_key: :sponsorable_id,
        touch: true,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_ship)
    route = build_belongs_to_route(
      reflection,
      reference: { reference_key: :club_id, target_key: :pirate_id },
      constraints: { reference_key: :sponsorable_type, target_key: :name }
    )
    original_time = Time.utc(2000)
    decoy = Ship.create!(name: "Decoy constrained ship", pirate_id: 9_000_111, updated_at: original_time)
    old_ship = Ship.create!(name: "Old constrained ship", pirate_id: decoy.pirate_id, updated_at: original_time)
    new_ship = Ship.create!(name: "New constrained ship", pirate_id: 9_000_112, updated_at: original_time)
    reference_class.insert_all!([
      {
        club_id: old_ship.pirate_id,
        sponsorable_id: -1,
        sponsorable_type: old_ship.name,
      }
    ])
    reference = reference_class.find_by!(sponsorable_type: old_ship.name)

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      reference.club_id = new_ship.pirate_id
      reference.sponsorable_type = new_ship.name
      reference.save!
    end

    assert_operator old_ship.reload.updated_at, :>, original_time
    assert_equal original_time, decoy.reload.updated_at
  end

  def test_query_constraints_identify_async_destruction_destinations
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "ConstrainedAsyncReference"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :post_id,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    reflection.options[:dependent] = :destroy_async
    route = build_belongs_to_route(
      reflection,
      reference: { reference_key: :author_id, target_key: :author_id },
      constraints: { reference_key: :body, target_key: :title }
    )
    post = Post.create!(author_id: 9_000_121, title: "Async constrained post", body: "Async")
    reference = reference_class.create!(post_id: -1, author_id: post.author_id, body: post.title)
    association = reference.association(:routed_post)
    enqueued = nil

    reflection.stub(:association_route_resolver, fixed_resolver(route)) do
      association.stub(:enqueue_destroy_association, ->(**options) { enqueued = options }) do
        association.handle_dependency
      end
    end

    assert_equal Post.to_s, enqueued[:association_class]
    assert_equal [[post.title, post.author_id]], enqueued[:association_ids]
    assert_equal ["title", "author_id"], enqueued[:association_primary_key_column]
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

  private
    def fixed_resolver(route)
      TestAssociationRouteResolver.new(route)
    end

    def build_belongs_to_route(reflection, reference:, constraints:)
      reference = ActiveRecord::Key::Mapping.new(**reference)
      constraints = ActiveRecord::Key::Mapping.new(**constraints)

      ActiveRecord::AssociationRoute.new(
        destination_class: reflection.klass,
        link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints),
        reference_on: :origin
      )
    end

    def build_route(reflection, reference:, constraints:)
      reference = ActiveRecord::Key::Mapping.new(**reference)
      constraints = ActiveRecord::Key::Mapping.new(**constraints)

      ActiveRecord::AssociationRoute.new(
        destination_class: reflection.klass,
        link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints),
        reference_on: :destination
      )
    end
end
