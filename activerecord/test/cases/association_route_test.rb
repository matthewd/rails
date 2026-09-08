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
require "models/cpk"
require "models/book"
require "models/hardback"

class AssociationRouteTest < ActiveRecord::TestCase
  fixtures :items, :posts, :taggings

  class ThroughAutosaveDestination < ActiveRecord::Base
    self.table_name = "companies"
    self.inheritance_column = nil
    self.primary_key = :firm_id
  end

  class NullableRoutePost < ActiveRecord::Base
    self.table_name = "posts"
    self.inheritance_column = nil
    serialize :type, coder: YAML, type: Hash

    has_many :nullable_route_comments,
      class_name: "AssociationRouteTest::NullableRouteComment",
      foreign_key: :post_id
  end

  class NullableRouteComment < ActiveRecord::Base
    self.table_name = "comments"
    self.inheritance_column = nil
    serialize :type, coder: YAML, type: Hash
  end

  class RoutedItem < Item
    has_one :routed_item,
      through: :tagging,
      source: :taggable,
      source_type: "Item"
  end

  class InverseAliasedRouteComment < ActiveRecord::Base
    self.table_name = "comments"
    self.inheritance_column = nil

    alias_attribute :route_fk, :post_id
  end

  class InverseRemappedRouteComment < InverseAliasedRouteComment
    alias_attribute :route_fk, :author_id
  end

  class ConstrainedRouteMember < ActiveRecord::Base
    self.table_name = "members"

    has_one :routed_sponsor,
      -> { order(:id) },
      as: :sponsorable,
      class_name: "Sponsor"
  end

  class ConstrainedRoutePost < ActiveRecord::Base
    self.table_name = "posts"
    self.inheritance_column = nil

    has_many :route_links,
      class_name: "AssociationRouteTest::ConstrainedRouteLink",
      foreign_key: :sponsorable_id
    has_many :route_ships, through: :route_links, source: :ship
  end

  class ConstrainedRouteLink < ActiveRecord::Base
    self.table_name = "sponsors"

    belongs_to :ship,
      class_name: "Ship",
      foreign_key: :club_id,
      primary_key: :pirate_id,
      optional: true
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

  class SerializedRouteLink < ActiveRecord::Base
    self.table_name = "sponsors"
    serialize :sponsorable_type, coder: YAML, type: Hash

    belongs_to :ship,
      class_name: "Ship",
      foreign_key: :club_id,
      primary_key: :pirate_id,
      optional: true
  end

  class SerializedThroughRoutePost < ActiveRecord::Base
    self.table_name = "posts"
    self.inheritance_column = nil
    serialize :type, coder: YAML, type: Hash

    has_many :serialized_route_links,
      class_name: "AssociationRouteTest::SerializedRouteLink",
      foreign_key: :sponsor_id
    has_many :serialized_route_ships,
      through: :serialized_route_links,
      source: :ship
  end

  class BooleanRouteBook < ActiveRecord::Base
    self.table_name = "books"

    has_many :boolean_route_comments,
      class_name: "Comment",
      foreign_key: :author_type,
      primary_key: :boolean_status
  end

  class ArrayRouteRecord < ActiveRecord::Base
    self.table_name = "bigint_array"

    belongs_to :same_row,
      class_name: "AssociationRouteTest::ArrayRouteRecord",
      foreign_key: :id,
      optional: true
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
    origin_class = Class.new(NullableRoutePost) do
      def self.name = "AliasedRoutePost"

      alias_attribute :route_key, :id
      has_many :routed_comments,
        class_name: "AssociationRouteTest::InverseAliasedRouteComment",
        foreign_key: :route_fk,
        primary_key: :route_key,
        inverse_of: false
    end
    subclass = Class.new(origin_class) do
      def self.name = "RemappedRoutePost"

      alias_attribute :route_key, :author_id
    end
    owner = subclass.create!(
      id: 9_000_401,
      author_id: 9_000_402,
      title: "Inverse alias",
      body: "Inverse alias"
    )
    comment = InverseAliasedRouteComment.create!(post_id: owner.author_id, body: "Inverse alias comment")
    remapped = InverseRemappedRouteComment.new(body: "Remapped inverse alias")
    owner.association(:routed_comments).send(:set_owner_attributes, remapped)

    assert_equal owner.author_id, owner.routed_comments.build.post_id
    assert_equal owner.author_id, remapped.author_id
    assert_nil remapped.post_id
    assert_equal [comment], owner.routed_comments.reload.to_a
    joined = subclass.where(id: owner.id).joins(:routed_comments)
    assert_predicate joined, :exists?, joined.to_sql
    assert_equal [comment], subclass.eager_load(:routed_comments).find(owner.id).routed_comments
  end

  def test_routes_are_cached_by_concrete_query_constraints
    reflection = Client.reflect_on_association(:firm)
    first_class = Class.new(Client)
    second_class = Class.new(Client)
    first_constraints = ActiveRecord::Key::Mapping.new(reference_key: :name, target_key: :name)
    second_constraints = ActiveRecord::Key::Mapping.new(reference_key: :rating, target_key: :rating)
    constraints_for = lambda do |reference_class, _target_class|
      if reference_class == first_class
        first_constraints
      elsif reference_class == second_class
        second_constraints
      else
        ActiveRecord::Key::Mapping.empty
      end
    end

    reflection.clear_association_scope_cache
    reflection.stub(:association_route_constraints, constraints_for) do
      first_route = reflection.association_route_for_origin(first_class, Firm)
      second_route = reflection.association_route_for_origin(second_class, Firm)

      assert_equal first_constraints, first_route.link.constraints
      assert_equal second_constraints, second_route.link.constraints
    end
  ensure
    reflection&.clear_association_scope_cache
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

  def test_inverse_polymorphic_route_keeps_constraints
    member = ConstrainedRouteMember.create!(member_type_id: 42)
    Sponsor.create!(club_id: 41, sponsorable: member)
    matching = Sponsor.create!(club_id: 42, sponsorable: member)
    reflection = ConstrainedRouteMember.reflect_on_association(:routed_sponsor)

    with_constraints(reflection, reference_key: :club_id, target_key: :member_type_id) do
      assert_equal matching, member.routed_sponsor
    end
  end

  def test_internal_query_constraints_apply_to_reads_but_not_writes
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :body, target_key: :title }
    )
    post = posts(:welcome)
    matching = Comment.create!(post_id: post.id, body: post.title)
    mismatching = Comment.create!(post_id: post.id, body: "Not the post title")

    reflection.stub(:association_route, route) do
      post.comments.load
      post.title = mismatching.body
      assert_not_predicate post.association(:comments), :stale_target?
      post.title = matching.body
      post.association(:comments).reset

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

  def test_preloader_keeps_false_reference_values_after_type_conversion
    book = BooleanRouteBook.create!(boolean_status: false)
    comment = Comment.create!(post_id: -1, body: "Boolean route", author_type: "false")

    ActiveRecord::Associations::Preloader.new(records: [book], associations: :boolean_route_comments).call

    assert_equal [comment], book.boolean_route_comments
  end

  def test_preloader_converts_each_match_key_pair_independently
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "TypedRouteComment"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :body,
        primary_key: :id,
        optional: true
    end
    post = Post.create!(author_id: 9_000_201, title: "Typed route", body: "Typed route")
    reference = reference_class.create!(post_id: -1, author_id: post.author_id, body: post.id.to_s)
    reflection = reference_class.reflect_on_association(:routed_post)
    route = build_belongs_to_route(
      reference: { reference_key: :body, target_key: :id },
      constraints: { reference_key: :author_id, target_key: :author_id }
    )

    reflection.stub(:association_route, route) do
      ActiveRecord::Associations::Preloader.new(
        records: [reference],
        associations: :routed_post,
        available_records: [post]
      ).call
    end

    assert_equal post, reference.routed_post
  end

  def test_nullable_query_constraint_uses_an_uncached_association_scope
    post = NullableRoutePost.create!(title: "Nullable constraint", body: "Nullable constraint")
    comment = NullableRouteComment.create!(post_id: post.id, body: "Nullable constraint")
    reflection = NullableRoutePost.reflect_on_association(:nullable_route_comments)
    route = build_route(
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :deleted_at, target_key: :deleted_at }
    )

    reflection.stub(:association_route, route) do
      reflection.stub(:association_scope_cache, ->(*) { flunk("nullable constraint used the statement cache") }) do
        assert_equal [comment], post.nullable_route_comments.to_a
      end
    end
  end

  def test_serialized_null_query_constraint_uses_an_uncached_association_scope
    post = NullableRoutePost.create!(title: "Serialized constraint", body: "Serialized constraint", type: {})
    comment = NullableRouteComment.create!(post_id: post.id, body: "Serialized constraint", type: {})
    other_type = { "other" => true }
    other_post = NullableRoutePost.create!(title: "Other constraint", body: "Other constraint", type: other_type)
    other_comment = NullableRouteComment.create!(post_id: other_post.id, body: "Other constraint", type: other_type)
    reflection = NullableRoutePost.reflect_on_association(:nullable_route_comments)
    route = build_route(
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :type, target_key: :type }
    )

    reflection.stub(:association_route, route) do
      reflection.stub(:association_scope_cache, ->(*) { flunk("serialized null constraint used the statement cache") }) do
        assert_equal [comment], post.nullable_route_comments.to_a
      end

      owners = NullableRoutePost.where(id: [post.id, other_post.id]).order(:id).to_a
      ActiveRecord::Associations::Preloader.new(records: owners, associations: :nullable_route_comments).call

      assert_equal [[comment.id], [other_comment.id]], owners.map { |owner| owner.nullable_route_comments.map(&:id) }
    end
  end

  def test_through_constraint_uses_the_intermediate_model_type_for_cache_safety
    post = SerializedThroughRoutePost.create!(title: "Serialized through", body: "Serialized through", type: {})
    ship = Ship.create!(pirate_id: 9_000_421, name: "Serialized through ship")
    SerializedRouteLink.create!(sponsor_id: post.id, club_id: ship.pirate_id, sponsorable_type: {})
    through_reflection = SerializedThroughRoutePost.reflect_on_association(:serialized_route_links)
    reflection = SerializedThroughRoutePost.reflect_on_association(:serialized_route_ships)
    route = build_route(
      reference: { reference_key: :sponsor_id, target_key: :id },
      constraints: { reference_key: :sponsorable_type, target_key: :type }
    )

    through_reflection.stub(:association_route, route) do
      reflection.stub(:association_scope_cache, ->(*) { flunk("serialized null constraint used the statement cache") }) do
        assert_equal [ship], post.serialized_route_ships.to_a
      end
    end
  end

  def test_nullable_query_constraint_rebuilds_a_reset_association_scope
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "ResetConstrainedComment"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :author_id,
        primary_key: :author_id,
        optional: true
    end
    key = 9_001_001
    old_time = Time.utc(2000)
    old_target = Post.create!(author_id: key, deleted_at: old_time, title: "Old", body: "Old")
    current_target = Post.create!(author_id: key, deleted_at: nil, title: "Current", body: "Current")
    reference = reference_class.create!(post_id: -1, author_id: key, deleted_at: old_time, body: "Reference")
    reflection = reference_class.reflect_on_association(:routed_post)
    route = build_belongs_to_route(
      reference: { reference_key: :author_id, target_key: :author_id },
      constraints: { reference_key: :deleted_at, target_key: :deleted_at }
    )

    reflection.stub(:association_route, route) do
      association = reference.association(:routed_post)
      assert_equal old_target, reference.routed_post

      reference.deleted_at = nil
      association.reset

      assert_equal current_target, association.load_target
    end
  end

  def test_empty_association_array_predicate_is_false
    assert_empty Client.where(firm: [])
  end

  def test_association_predicate_combines_scalar_ids_and_nil_in_either_order
    firm = Firm.create!(name: "Predicate firm")
    associated = Client.create!(name: "Associated client", firm: firm)
    unassociated = Client.new(name: "Unassociated client")
    unassociated.save!(validate: false)
    expected = [associated, unassociated].sort_by(&:id)

    assert_equal expected, Client.where(firm: [nil, firm.id]).order(:id).to_a
    assert_equal expected, Client.where(firm: [firm.id, nil]).order(:id).to_a
  end

  def test_non_null_query_constraint_rebuilds_a_reset_association_scope
    member = ConstrainedRouteMember.create!(member_type_id: 41)
    old_target = Sponsor.create!(club_id: 41, sponsorable: member)
    current_target = Sponsor.create!(club_id: 42, sponsorable: member)
    reflection = ConstrainedRouteMember.reflect_on_association(:routed_sponsor)

    with_constraints(reflection, reference_key: :club_id, target_key: :member_type_id) do
      association = member.association(:routed_sponsor)
      assert_equal old_target, association.load_target

      member.member_type_id = 42
      association.reset

      assert_equal current_target, association.load_target
    end
  end

  def test_nil_association_predicate_uses_only_the_reference
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "NilPredicateReference"

      belongs_to :routed_ship,
        class_name: "Ship",
        foreign_key: :club_id,
        primary_key: :pirate_id,
        optional: true
    end
    reference = reference_class.create!(club_id: nil, sponsorable_type: "Present constraint")
    reflection = reference_class.reflect_on_association(:routed_ship)
    route = build_belongs_to_route(
      reference: { reference_key: :club_id, target_key: :pirate_id },
      constraints: { reference_key: :sponsorable_type, target_key: :name }
    )

    reflection.stub(:association_route, route) do
      assert_includes reference_class.where(routed_ship: nil), reference
      assert_includes reference_class.where(routed_ship: [nil]), reference

      target = Ship.new(pirate_id: nil, name: reference.sponsorable_type)
      assert_equal reference, reference_class.where(routed_ship: target).first
      assert_equal reference, reference_class.find_by(routed_ship: target)
    end
  end

  def test_scalar_association_predicate_uses_only_the_reference
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "ScalarPredicateReference"

      belongs_to :routed_ship,
        class_name: "Ship",
        foreign_key: :club_id,
        primary_key: :pirate_id,
        optional: true
    end
    target = Ship.create!(pirate_id: 9_000_051, name: "Scalar predicate")
    reference = reference_class.create!(club_id: target.pirate_id, sponsorable_type: "Different constraint")
    reflection = reference_class.reflect_on_association(:routed_ship)
    route = build_belongs_to_route(
      reference: { reference_key: :club_id, target_key: :pirate_id },
      constraints: { reference_key: :sponsorable_type, target_key: :name }
    )

    reflection.stub(:association_route, route) do
      assert_equal reference, reference_class.where(routed_ship: target.pirate_id).first
      assert_equal reference, reference_class.find_by(routed_ship: target.pirate_id)
    end
  end

  def test_polymorphic_relation_predicate_uses_the_complete_match
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "ConstrainedPolymorphicReference"

      belongs_to :subject,
        polymorphic: true,
        foreign_key: :club_id,
        foreign_type: :sponsorable_type,
        optional: true
    end
    post = Post.create!(title: "Polymorphic relation", body: "Polymorphic relation")
    reference = reference_class.create!(club_id: post.id, sponsor_type: post.title, sponsorable_type: Post.polymorphic_name)
    reflection = reference_class.reflect_on_association(:subject)

    with_constraints(reflection, reference_key: :sponsor_type, target_key: :title) do
      assert_equal [reference], reference_class.where(subject: Post.where(id: post.id)).to_a
      assert_empty reference_class.where(subject: Post.none)
    end
  end

  def test_empty_constrained_association_predicates_are_false
    reflection = Comment.reflect_on_association(:post)
    route = build_belongs_to_route(
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :body, target_key: :title }
    )

    reflection.stub(:association_route, route) do
      assert_empty Comment.where(post: [])
      assert_empty Comment.where(post: Post.none)
    end
  end

  def test_empty_association_predicate_is_false_for_array_constraint
    skip unless current_adapter?(:PostgreSQLAdapter)

    ArrayRouteRecord.create!(big_int_data_points: [])
    reflection = ArrayRouteRecord.reflect_on_association(:same_row)

    with_constraints(reflection, reference_key: :big_int_data_points, target_key: :big_int_data_points) do
      assert_empty ArrayRouteRecord.where(same_row: [])
      assert_empty ArrayRouteRecord.where(same_row: ArrayRouteRecord.none)
    end
  end

  def test_find_by_preserves_array_valued_query_constraint
    skip unless current_adapter?(:PostgreSQLAdapter)

    record = ArrayRouteRecord.create!(big_int_data_points: [10, 20])
    reflection = ArrayRouteRecord.reflect_on_association(:same_row)

    with_constraints(reflection, reference_key: :big_int_data_points, target_key: :big_int_data_points) do
      assert_equal record, ArrayRouteRecord.where(same_row: record).first
      assert_equal record, ArrayRouteRecord.find_by(same_row: record)
    end
  end

  def test_disable_joins_constraints_do_not_become_creation_defaults
    author = Author.create!(name: "Constrained author")
    post = Post.create!(author: author, title: "Constrained post", body: "Post body")
    comment = Comment.create!(post: post, body: post.title)
    reflection = Post.reflect_on_association(:comments)
    route = build_route(
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :body, target_key: :title }
    )

    reflection.stub(:association_route, route) do
      scope = author.association(:no_joins_comments).scope
      assert_equal [comment], scope.to_a
      assert_not scope.scope_for_create.key?("body")
    end
  end

  def test_query_constraints_identify_counter_cache_destinations
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "ConstrainedCounterReference"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :author_id,
        primary_key: :author_id,
        counter_cache: :legacy_comments_count,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    route = build_belongs_to_route(
      reference: { reference_key: :author_id, target_key: :author_id },
      constraints: { reference_key: :deleted_at, target_key: :deleted_at }
    )
    old_post = Post.create!(author_id: 9_000_101, title: "Old constrained post", body: "Old")
    decoy = Post.create!(author_id: old_post.author_id, title: "Decoy constrained post", body: "Decoy", deleted_at: Time.utc(2000))
    new_post = Post.create!(author_id: 9_000_102, title: "New constrained post", body: "New", deleted_at: Time.utc(2001))
    old_post.update_column(:legacy_comments_count, 1)
    decoy.update_column(:legacy_comments_count, 1)
    reference_class.insert_all!([
      {
        post_id: -1,
        author_id: old_post.author_id,
        body: "Constrained reference",
        deleted_at: old_post.deleted_at,
      }
    ])
    reference = reference_class.find_by!(author_id: old_post.author_id, deleted_at: nil)

    reflection.stub(:association_route, route) do
      assert_equal reference, reference_class.where(routed_post: old_post).first
      assert_equal reference, reference_class.find_by(routed_post: old_post)

      reference.author_id = new_post.author_id
      reference.deleted_at = new_post.deleted_at
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
        foreign_key: :club_id,
        primary_key: :pirate_id,
        touch: true,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_ship)
    route = build_belongs_to_route(
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

    reflection.stub(:association_route, route) do
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
        foreign_key: :author_id,
        primary_key: :author_id,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    reflection.options[:dependent] = :destroy_async
    route = build_belongs_to_route(
      reference: { reference_key: :author_id, target_key: :author_id },
      constraints: { reference_key: :body, target_key: :title }
    )
    post = Post.create!(author_id: 9_000_121, title: "Async constrained post", body: "Async")
    reference = reference_class.create!(post_id: -1, author_id: post.author_id, body: post.title)
    association = reference.association(:routed_post)
    enqueued = nil

    reflection.stub(:association_route, route) do
      association.stub(:enqueue_destroy_association, ->(**options) { enqueued = options }) do
        association.handle_dependency
      end
    end

    assert_equal Post.to_s, enqueued[:association_class]
    assert_equal [[post.title, post.author_id]], enqueued[:association_ids]
    assert_equal ["title", "author_id"], enqueued[:association_primary_key_column]
  end

  def test_async_destruction_uses_the_loaded_destination_match
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "LoadedAsyncReference"

      belongs_to :routed_post,
        class_name: "Post",
        foreign_key: :author_id,
        primary_key: :author_id,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    reflection.options[:dependent] = :destroy_async
    route = build_belongs_to_route(
      reference: { reference_key: :author_id, target_key: :author_id },
      constraints: { reference_key: :body, target_key: :title }
    )
    original = Post.create!(author_id: 9_000_131, title: "Original", body: "Original")
    replacement = Post.create!(author_id: original.author_id, title: "Replacement", body: "Replacement")
    reference = reference_class.create!(post_id: -1, author_id: original.author_id, body: original.title)
    association = reference.association(:routed_post)
    enqueued = nil

    persisted_title = replacement.title
    persisted_author_id = replacement.author_id
    reflection.stub(:association_route, route) do
      reference.routed_post = replacement
      replacement.title = "Unsaved title"
      replacement.author_id = 9_000_132
      association.stub(:enqueue_destroy_association, ->(**options) { enqueued = options }) do
        association.handle_dependency
      end
    end

    assert_equal [[persisted_title, persisted_author_id]], enqueued[:association_ids]
  end

  def test_async_destruction_falls_back_to_owner_values_for_unselected_columns
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil

      def self.name = "PartialAsyncComment"

      belongs_to :routed_post,
        -> { select(:title) },
        class_name: "Post",
        foreign_key: :post_id,
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_post)
    reflection.options[:dependent] = :destroy_async
    post = Post.create!(title: "Partial async target", body: "Partial async target")
    reference = reference_class.create!(post_id: post.id, body: "Partial async reference")
    association = reference.association(:routed_post)
    enqueued = nil

    association.load_target
    association.stub(:enqueue_destroy_association, ->(**options) { enqueued = options }) do
      association.handle_dependency
    end

    assert_equal [post.id], enqueued[:association_ids]
  end

  def test_async_destruction_reads_physical_composite_key_columns
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "cpk_reviews"

      def self.name = "CompositeAsyncReference"

      belongs_to :routed_book,
        class_name: "Cpk::Book",
        foreign_key: [:author_id, :number],
        primary_key: [:author_id, :id],
        optional: true
    end
    reflection = reference_class.reflect_on_association(:routed_book)
    reflection.options[:dependent] = :destroy_async
    book = Cpk::Book.create!(id: [9_000_141, 9_000_142])
    reference = reference_class.create!(author_id: book.author_id, number: book.id_value)
    association = reference.association(:routed_book)
    enqueued = nil

    association.load_target
    association.stub(:enqueue_destroy_association, ->(**options) { enqueued = options }) do
      association.handle_dependency
    end

    assert_equal [[book.author_id, book.id_value]], enqueued[:association_ids]
  end

  def test_through_deletion_uses_the_complete_match
    post = ConstrainedRoutePost.create!(title: "Constrained through", body: "Constrained through")
    first = Ship.create!(pirate_id: 9_000_301, name: "First constrained ship")
    second = Ship.create!(pirate_id: first.pirate_id, name: "Second constrained ship")
    first_link = ConstrainedRouteLink.create!(sponsorable_id: post.id, club_id: first.pirate_id, sponsor_type: first.name)
    second_link = ConstrainedRouteLink.create!(sponsorable_id: post.id, club_id: second.pirate_id, sponsor_type: second.name)
    reflection = ConstrainedRouteLink.reflect_on_association(:ship)

    with_constraints(reflection, reference_key: :sponsor_type, target_key: :name) do
      assert_equal [first, second], post.route_ships.order(:id).to_a
      post.route_links.load
      post.route_ships.delete(first)
    end

    assert_not ConstrainedRouteLink.exists?(first_link.id)
    assert ConstrainedRouteLink.exists?(second_link.id)
    assert_equal [second_link], post.route_links.to_a
  end

  def test_deleting_an_unsaved_through_target_preserves_other_join_records
    post = Post.new(title: "Unsaved through", body: "Unsaved through")
    first = Tag.new(name: "First unsaved tag")
    second = Tag.new(name: "Second unsaved tag")

    post.tags.concat(first, second)
    assert_equal [first, second], post.taggings.map(&:tag)

    post.tags.delete(first)

    assert_equal [second], post.taggings.map(&:tag)
  end

  def test_sti_source_type_deletion_updates_the_loaded_through_cache
    author = Author.create!(name: "STI source type author")
    author.books.load
    hardback = BestHardback.create!
    author.best_hardbacks << hardback
    join = author.books.find { |book| book.format_record == hardback }

    author.best_hardbacks.delete(hardback)

    assert_not Book.exists?(join.id)
    assert_not_includes author.books, join
  end

  def test_through_deletion_matches_loaded_keys_after_type_conversion
    post = ConstrainedRoutePost.create!(title: "Typed through", body: "Typed through")
    first = Ship.create!(pirate_id: 9_000_321, name: "First typed ship")
    second = Ship.create!(pirate_id: first.pirate_id, name: "Second typed ship")
    first_link = ConstrainedRouteLink.create!(
      sponsorable_id: post.id,
      sponsorable_type: first.id.to_s,
      club_id: first.pirate_id
    )
    second_link = ConstrainedRouteLink.create!(
      sponsorable_id: post.id,
      sponsorable_type: second.id.to_s,
      club_id: second.pirate_id
    )
    reflection = ConstrainedRouteLink.reflect_on_association(:ship)
    route = build_belongs_to_route(
      reference: { reference_key: :club_id, target_key: :pirate_id },
      constraints: { reference_key: :sponsorable_type, target_key: :id }
    )

    post.route_links.load
    reflection.stub(:association_route, route) do
      post.route_ships.delete(first)
    end

    assert_not ConstrainedRouteLink.exists?(first_link.id)
    assert ConstrainedRouteLink.exists?(second_link.id)
    assert_equal [second_link], post.route_links.to_a
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

  private
    def with_constraints(reflection, reference_key:, target_key:, &block)
      constraints = ActiveRecord::Key::Mapping.new(reference_key: reference_key, target_key: target_key)
      reflection.clear_association_scope_cache unless reflection.polymorphic?
      reflection.stub(:association_route_constraints, constraints, &block)
    ensure
      reflection.clear_association_scope_cache unless reflection.polymorphic?
    end

    def build_belongs_to_route(reference:, constraints:)
      reference = ActiveRecord::Key::Mapping.new(**reference)
      constraints = ActiveRecord::Key::Mapping.new(**constraints)

      ActiveRecord::AssociationRoute.new(
        link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints),
        reference_on: :origin
      )
    end

    def build_route(reference:, constraints:)
      reference = ActiveRecord::Key::Mapping.new(**reference)
      constraints = ActiveRecord::Key::Mapping.new(**constraints)

      ActiveRecord::AssociationRoute.new(
        link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints),
        reference_on: :destination
      )
    end
end
