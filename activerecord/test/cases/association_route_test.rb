# frozen_string_literal: true

require "cases/helper"
require "active_support/core_ext/object/with"
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

  class SelfRoutedCompany < ActiveRecord::Base
    self.table_name = "companies"
    self.inheritance_column = nil

    belongs_to :parent, class_name: name, foreign_key: :firm_id, optional: true
    belongs_to :mentor, class_name: name, foreign_key: :client_of, optional: true
    has_one :parents_mentor, through: :parent, source: :mentor
    has_one :scoped_parents_mentor, ->(owner) { where.not(name: owner.name) }, through: :parent, source: :mentor
    has_one :mentors_parent, through: :mentor, source: :parent
    has_one :parents_mentors_parent, through: :parent, source: :mentors_parent
  end

  class NullableRouteComment < ActiveRecord::Base
    self.table_name = "comments"
    self.inheritance_column = nil
    serialize :type, coder: YAML, type: Hash
  end

  class DoubleAliasedRouteComment < NullableRouteComment
    alias_attribute :route_fk, :post_id
    alias_attribute :post_id, :author_id

    belongs_to :routed_post, class_name: "Post", foreign_key: :route_fk, optional: true
  end

  class RoutedItem < Item
    has_one :routed_item,
      through: :tagging,
      source: :taggable,
      source_type: "Item"
  end

  class ConstrainedRouteMember < ActiveRecord::Base
    self.table_name = "members"

    has_one :routed_sponsor,
      -> { order(:id) },
      as: :sponsorable,
      class_name: "Sponsor"
  end

  class ArrayRouteRecord < ActiveRecord::Base
    self.table_name = "bigint_array"

    belongs_to :same_row,
      class_name: "AssociationRouteTest::ArrayRouteRecord",
      foreign_key: :id,
      optional: true
    belongs_to :same_array,
      class_name: "AssociationRouteTest::ArrayRouteRecord",
      foreign_key: [:big_int_data_points],
      primary_key: [:big_int_data_points],
      optional: true
  end

  def test_association_link_combines_constraints_with_the_reference
    reference = build_mapping(reference_key: :post_id, target_key: :id)
    constraints = build_mapping(reference_key: :account_id, target_key: :account_id)
    link = ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints)

    assert_equal [["post_id", "id"]], link.reference.to_a
    assert_equal [["account_id", "account_id"]], link.constraints.to_a
    assert_equal [
      ["account_id", "account_id"],
      ["post_id", "id"],
    ], link.match.to_a
  end

  def test_unconstrained_links_reuse_the_reference_and_empty_mapping
    empty = ActiveRecord::Key::Mapping.empty
    reference = build_mapping(reference_key: :post_id, target_key: :id)

    2.times do
      link = ActiveRecord::AssociationLink.new(reference: reference)

      assert_same empty, link.constraints
      assert_same reference, link.match
    end
  end

  def test_links_normalize_match_keys_without_changing_reference_shape
    [[:post_id, [:id]], [[:post_id], :id], [[:post_id], [:id]]].each do |reference_key, target_key|
      reference = build_mapping(reference_key: reference_key, target_key: target_key)
      link = ActiveRecord::AssociationLink.new(reference: reference)
      route = ActiveRecord::AssociationRoute.new(link: link)

      assert_same reference, link.reference
      assert_same reference.reference_key, route.reference_origin_key
      assert_same reference.target_key, route.reference_destination_key
      assert_equal "post_id", route.origin_key.name
      assert_equal "id", route.destination_key.name
      assert_equal [["post_id", "id"]], link.match.to_a
    end
  end

  def test_unconstrained_routes_share_frozen_match_and_reference_pairs
    keys = [
      [:post_id, :id],
      [[:post_id], :id],
      [:post_id, [:id]],
      [[:post_id], [:id]],
      [[:tenant_id, :post_id], [:tenant_id, :id]],
      [nil, nil],
      [[], []],
      [nil, []],
      [[], nil],
    ]

    keys.each do |reference_name, target_name|
      reference = build_mapping(reference_key: reference_name, target_key: target_name)
      link = ActiveRecord::AssociationLink.new(reference: reference)
      [ActiveRecord::AssociationRoute, ActiveRecord::AssociationRoute::Reverse].each do |route_class|
        route = route_class.new(link: link)
        expected = reference.to_a
        expected = expected.map(&:reverse) if route_class == ActiveRecord::AssociationRoute::Reverse
        matches = route.each_match { }
        references = route.each_reference { }

        assert_equal expected, matches
        assert_same matches, references
        assert_predicate matches, :frozen?
        assert(matches.all?(&:frozen?))
        assert_same reference.reference_key, route.link.reference.reference_key
        assert_same reference.target_key, route.link.reference.target_key
      end
    end
  end

  def test_constrained_routes_keep_match_and_reference_pairs_distinct
    link = ActiveRecord::AssociationLink.new(
      reference: build_mapping(reference_key: :post_id, target_key: :id),
      constraints: build_mapping(reference_key: :blog_id, target_key: :tenant_id)
    )

    [ActiveRecord::AssociationRoute, ActiveRecord::AssociationRoute::Reverse].each do |route_class|
      route = route_class.new(link: link)
      expected_match = [["blog_id", "tenant_id"], ["post_id", "id"]]
      expected_reference = [["post_id", "id"]]
      if route_class == ActiveRecord::AssociationRoute::Reverse
        expected_match.map!(&:reverse)
        expected_reference.map!(&:reverse)
      end
      matches = route.each_match { }
      references = route.each_reference { }

      assert_equal expected_match, matches
      assert_equal expected_reference, references
      assert_not_same matches, references
      assert_predicate matches, :frozen?
      assert_predicate references, :frozen?
      assert(matches.all?(&:frozen?))
      assert(references.all?(&:frozen?))
    end
  end

  def test_unconstrained_routes_share_empty_constraint_pairs
    link = ActiveRecord::AssociationLink.new(reference: build_mapping(reference_key: :post_id, target_key: :id))
    forward = ActiveRecord::AssociationRoute.new(link: link)
    inverse = ActiveRecord::AssociationRoute::Reverse.new(link: link)

    pairs = forward.each_constraint { flunk "Yielded an empty constraint" }
    assert_empty pairs
    assert_same pairs, inverse.each_constraint { flunk "Yielded an empty constraint" }
    assert_predicate pairs, :frozen?
  end

  def test_belongs_to_route_is_physically_oriented_from_foreign_key
    reflection = Client.reflect_on_association(:firm)
    route = reflection.association_route

    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "client_of", route.origin_key.name
    assert_equal "id", route.destination_key.name
  end

  def test_has_many_route_preserves_physical_orientation_and_reverses_traversal
    reflection = Firm.reflect_on_association(:clients_of_firm)
    route = reflection.association_route

    assert_equal [["client_of", "id"]], route.link.reference.to_a
    assert_equal "id", route.origin_key.name
    assert_equal "client_of", route.destination_key.name
  end

  def test_default_non_polymorphic_routes_are_shared_with_explicit_declared_endpoints
    reflections = [
      Client.reflect_on_association(:firm),
      Firm.reflect_on_association(:clients_of_firm),
    ]

    reflections.each do |reflection|
      route = reflection.association_route
      assert_not_respond_to reflection, :association_route_for_origin
      assert_not_respond_to reflection, :association_route_for_target

      reflection.stub(:association_route_constraints, ->(*) { flunk "Rebuilt cached route constraints" }) do
        assert_same route, reflection.association_route(origin_class: reflection.active_record, destination_class: reflection.klass)
      end

      reflection.stub(:klass, -> { flunk "Resolved a cached route's destination again" }) do
        assert_same route, reflection.association_route
      end
    end
  end

  def test_variant_route_resolution_is_cached_by_endpoint_classes
    variants = [
      [Client.reflect_on_association(:firm), SpecialClient, Firm],
      [Member.reflect_on_association(:sponsor), Member, Sponsor],
      [Sponsor.reflect_on_association(:sponsorable), Sponsor, Member],
    ]

    variants.each do |reflection, origin_class, destination_class|
      route = reflection.association_route(origin_class: origin_class, destination_class: destination_class)

      reflection.stub(:association_route_constraints, ->(*) { flunk "Rebuilt cached route constraints" }) do
        assert_same route, reflection.association_route(origin_class: origin_class, destination_class: destination_class)
      end
    end
  end

  def test_cached_inverse_polymorphic_route_does_not_resolve_the_destination_again
    reflection = Member.reflect_on_association(:sponsor)
    route = reflection.association_route

    reflection.stub(:klass, -> { flunk "Resolved a cached route's destination again" }) do
      assert_same route, reflection.association_route
    end
  end

  def test_clearing_route_caches_discards_resolved_and_interned_routes
    reflection = Member.reflect_on_association(:sponsor)
    route = reflection.association_route

    reflection.clear_association_scope_cache
    rebuilt_route = reflection.association_route

    assert_not_same route, rebuilt_route
    assert_equal route.link.reference, rebuilt_route.link.reference
    assert_equal route.fixed_reference_values, rebuilt_route.fixed_reference_values
  ensure
    reflection&.clear_association_scope_cache
  end

  def test_polymorphic_route_caches_can_be_cleared_with_a_concrete_destination
    reflection = Sponsor.reflect_on_association(:sponsorable)
    route = reflection.association_route(destination_class: Member)

    reflection.clear_association_scope_cache(Member)
    rebuilt_route = reflection.association_route(destination_class: Member)

    assert_not_same route, rebuilt_route
    assert_equal route.link.reference, rebuilt_route.link.reference
    assert_equal route.fixed_reference_values, rebuilt_route.fixed_reference_values
  ensure
    reflection&.clear_association_scope_cache(Member)
  end

  def test_frozen_reflections_can_resolve_and_clear_route_caches
    [false, true].each do |warm|
      fresh_route_reflections.each do |reflection, destination_class|
        previous = reflection.association_route(destination_class: destination_class) if warm
        reflection.freeze
        route = reflection.association_route(destination_class: destination_class)

        assert_same previous, route if warm
        assert_same route, reflection.association_route(destination_class: destination_class)

        reflection.clear_association_scope_cache(destination_class)
        assert_not_same route, reflection.association_route(destination_class: destination_class)
      end
    end
  end

  def test_frozen_polymorphic_reflection_clears_unbound_and_concrete_routes
    reflection, destination_class = fresh_route_reflections.last
    reflection.freeze
    unbound = reflection.association_route
    concrete = reflection.association_route(destination_class: destination_class)

    reflection.clear_association_scope_cache(destination_class)

    assert_not_same unbound, reflection.association_route
    assert_not_same concrete, reflection.association_route(destination_class: destination_class)
  end

  def test_copied_reflections_have_independent_route_caches
    fresh_route_reflections.each do |reflection, destination_class|
      original = reflection.association_route(destination_class: destination_class)
      copy = reflection.dup
      copied = copy.association_route(destination_class: destination_class)

      assert_not_same original, copied
      assert_equal original.each_match.to_a, copied.each_match.to_a
      copy.clear_association_scope_cache(destination_class)
      assert_not_same copied, copy.association_route(destination_class: destination_class)
      assert_same original, reflection.association_route(destination_class: destination_class)
    end
  end

  def test_copies_of_frozen_reflections_have_independent_route_caches
    fresh_route_reflections.each do |reflection, destination_class|
      reflection.freeze
      original = reflection.association_route(destination_class: destination_class)

      [reflection.dup, reflection.clone, reflection.clone(freeze: false)].each do |copy|
        copied = copy.association_route(destination_class: destination_class)

        assert_not_same original, copied
        assert_equal original.each_match.to_a, copied.each_match.to_a
        copy.freeze
        assert_same copied, copy.association_route(destination_class: destination_class)
        copy.clear_association_scope_cache(destination_class)
        assert_not_same copied, copy.association_route(destination_class: destination_class)
        assert_same original, reflection.association_route(destination_class: destination_class)
      end
    end
  end

  if RUBY_VERSION >= "4.0"
    def test_route_caches_do_not_prevent_reflection_sharing_and_are_ractor_local
      [false, true].each do |warm|
        fresh_route_reflections.each do |reflection, destination_class|
          destination_class.initialize_find_by_cache
          original = reflection.association_route(destination_class: destination_class) if warm
          ActiveSupport::Ractors.make_shareable(reflection)
          original ||= reflection.association_route(destination_class: destination_class)

          reused, cleared, pairs = Ractor.new(reflection, destination_class) do |reflection, destination_class|
            route = reflection.association_route(destination_class: destination_class)
            same_route = route.equal?(reflection.association_route(destination_class: destination_class))
            reflection.clear_association_scope_cache(destination_class)
            new_route = !route.equal?(reflection.association_route(destination_class: destination_class))
            [same_route, new_route, route.each_match.to_a]
          end.value

          assert reused
          assert cleared
          assert_equal original.each_match.to_a, pairs
          assert_same original, reflection.association_route(destination_class: destination_class)
        end
      end
    end

    def test_equivalent_polymorphic_routes_are_interned_in_another_ractor
      reflection, = fresh_route_reflections.last
      Company.primary_key
      Firm.primary_key
      ActiveSupport::Ractors.make_shareable(reflection)

      shared = Ractor.new(reflection) do |reflection|
        company_route = reflection.association_route(destination_class: Company)
        firm_route = reflection.association_route(destination_class: Firm)
        company_route.equal?(firm_route)
      end.value

      assert shared
    end

  end

  def test_equivalent_inverse_polymorphic_routes_share_identity
    reflection = Member.reflect_on_association(:sponsor)
    origin_class = Class.new(Member) do
      def self.name = "EquivalentRouteMember"
    end
    variant = reflection.association_route(origin_class: origin_class)

    assert_same variant, reflection.association_route
  end

  def test_inverse_polymorphic_route_cache_can_be_invalidated_after_metadata_changes
    reflection = ConstrainedRouteMember.reflect_on_association(:routed_sponsor)
    full = ActiveRecord::Base.with(store_full_class_name: true) do
      reflection.clear_association_scope_cache
      reflection.association_route
    end
    short = ActiveRecord::Base.with(store_full_class_name: false) do
      reflection.clear_association_scope_cache
      reflection.association_route(origin_class: ConstrainedRouteMember, destination_class: Sponsor)
    end

    assert_equal({ "sponsorable_type" => ConstrainedRouteMember.name }, full.fixed_reference_values)
    assert_equal({ "sponsorable_type" => "ConstrainedRouteMember" }, short.fixed_reference_values)
    assert_not_same full, short
  ensure
    reflection&.clear_association_scope_cache
  end

  def test_inverse_route_constraints_receive_the_oriented_endpoint_classes
    reflection = Member.reflect_on_association(:sponsor)
    origin_class = Class.new(Member) do
      def self.name = "ConcreteRouteMember"
    end
    destination_class = Class.new(Sponsor) do
      def self.name = "ConcreteRouteSponsor"
    end
    constraints_for = lambda do |reference_class, target_class|
      assert_same destination_class, reference_class
      assert_same origin_class, target_class
      ActiveRecord::Key::Mapping.empty
    end

    reflection.stub(:association_route_constraints, constraints_for) do
      reflection.association_route(origin_class: origin_class, destination_class: destination_class)
    end
  end

  def test_runtime_reflection_keeps_its_resolved_join_route
    association = Client.new.association(:firm)
    route = association.association_route
    reflection = ActiveRecord::Reflection::RuntimeReflection.new(association.reflection, association, route)

    assert_same route, reflection.association_route_for_join(Client, destination_class: Firm)
  end

  def test_belongs_to_assignment_uses_the_assigned_subclass_key
    target_class = Class.new(Firm) do
      def self.name = "AlternatePrimaryKeyFirm"

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

  def test_routes_are_cached_by_concrete_query_constraints
    reflection = Client.reflect_on_association(:firm)
    first_class = Class.new(Client) do
      def self.name = "FirstRouteClient"
    end
    second_class = Class.new(Client) do
      def self.name = "SecondRouteClient"
    end
    first_constraints = build_mapping(reference_key: :name, target_key: :name)
    second_constraints = build_mapping(reference_key: :rating, target_key: :rating)
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
      first_route = reflection.association_route(origin_class: first_class, destination_class: Firm)
      second_route = reflection.association_route(origin_class: second_class, destination_class: Firm)

      assert_equal first_constraints, first_route.link.constraints
      assert_equal second_constraints, second_route.link.constraints
    end
  ensure
    reflection&.clear_association_scope_cache
  end

  def test_polymorphic_route_resolves_the_target_key_for_each_destination_class
    reflection = PolymorphicComment.reflect_on_association(:person)
    author_route = reflection.association_route(destination_class: Author)
    person_route = reflection.association_route(destination_class: Person)

    assert_equal [["person_id", "author_code"]], author_route.link.reference.to_a
    assert_equal [["person_id", "external_id"]], person_route.link.reference.to_a
    assert_equal({ "person_type" => Author.polymorphic_name }, author_route.fixed_reference_values)
    assert_equal({ "person_type" => Person.polymorphic_name }, person_route.fixed_reference_values)
  end

  def test_inverse_polymorphic_route_has_the_same_physical_shape
    forward = Sponsor.reflect_on_association(:sponsorable).association_route(destination_class: Member)
    inverse = Member.reflect_on_association(:sponsor).association_route

    assert_equal forward.link.reference, inverse.link.reference
    assert_equal forward.origin_key, inverse.destination_key
    assert_equal forward.destination_key, inverse.origin_key
    assert_equal forward.fixed_reference_values, inverse.fixed_reference_values
    assert_empty forward.destination_fixed_values
    assert_equal inverse.fixed_reference_values, inverse.destination_fixed_values
  end

  def test_through_reflections_reject_single_edge_route_requests
    reflection = SelfRoutedCompany.reflect_on_association(:parents_mentor)

    assert_raises(ArgumentError) { reflection.association_route }
    assert_raises(ArgumentError) do
      reflection.association_route(origin_class: SelfRoutedCompany, destination_class: SelfRoutedCompany)
    end
    assert_not_respond_to reflection, :association_route_for_origin
    assert_not_respond_to reflection, :association_route_for_target
  end

  def test_source_route_is_only_exposed_on_through_reflections
    reflection = SelfRoutedCompany.reflect_on_association(:parents_mentor)

    assert_not_respond_to reflection.source_reflection, :source_association_route
    assert_equal [["client_of", "id"]], reflection.source_association_route.link.reference.to_a
  end

  def test_nested_source_route_resolves_the_terminal_edge
    reflection = SelfRoutedCompany.reflect_on_association(:parents_mentors_parent)

    assert_predicate reflection.source_reflection, :through_reflection?
    assert_equal [["firm_id", "id"]], reflection.source_association_route.link.reference.to_a
  end

  def test_source_route_preserves_the_through_source_type
    reflection = Tag.reflect_on_association(:tagged_posts)
    route = reflection.source_association_route

    assert_equal [["taggable_id", "id"]], route.link.reference.to_a
    assert_equal({ "taggable_type" => Post.polymorphic_name }, route.fixed_reference_values)
  end

  def test_through_route_chain_distinguishes_edges_with_the_same_endpoint_classes
    ancestor = SelfRoutedCompany.create!(name: "Mentor's parent")
    mentor = SelfRoutedCompany.create!(name: "Mentor", parent: ancestor)
    parent = SelfRoutedCompany.create!(name: "Parent", mentor: mentor)
    owner = SelfRoutedCompany.create!(name: "Owner", parent: parent)
    reflection = SelfRoutedCompany.reflect_on_association(:parents_mentor)
    routes = reflection.association_route_chain(origin_class: owner.class, destination_class: SelfRoutedCompany)

    assert_equal [[["client_of", "id"]], [["firm_id", "id"]]], routes.map { |route| route.link.reference.to_a }
    assert_equal mentor, owner.parents_mentor
    assert_equal [owner], SelfRoutedCompany.where(id: owner.id).joins(:parents_mentor).to_a
    assert_equal mentor, SelfRoutedCompany.eager_load(:parents_mentor).find(owner.id).parents_mentor
    assert_equal ancestor, owner.parents_mentors_parent
    assert_equal ancestor, SelfRoutedCompany.eager_load(:parents_mentors_parent).find(owner.id).parents_mentors_parent
  end

  def test_through_route_chain_preserves_the_supplied_destination_class
    destination_class = Class.new(SelfRoutedCompany) do
      def self.name = "AlternateRouteDestination"
      self.primary_key = :firm_id
    end
    reflection = SelfRoutedCompany.reflect_on_association(:parents_mentor)
    routes = reflection.association_route_chain(origin_class: SelfRoutedCompany, destination_class: destination_class)

    assert_equal [["client_of", "firm_id"]], routes.first.link.reference.to_a
    assert_equal [["firm_id", "id"]], routes.last.link.reference.to_a
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

  def test_inverse_polymorphic_route_keeps_constraints
    member = ConstrainedRouteMember.create!(member_type_id: 42)
    Sponsor.create!(club_id: 41, sponsorable: member)
    matching = Sponsor.create!(club_id: 42, sponsorable: member)
    reflection = ConstrainedRouteMember.reflect_on_association(:routed_sponsor)

    with_constraints(reflection, reference_key: :club_id, target_key: :member_type_id) do
      assert_equal matching, member.routed_sponsor
    end
  end

  def test_cached_association_reads_logical_inputs_from_each_owner
    first = posts(:welcome)
    second = Post.create!(title: "Second target", body: "Second target")
    reflection = DoubleAliasedRouteComment.reflect_on_association(:routed_post)
    routes = reflection.association_route_chain(origin_class: DoubleAliasedRouteComment, destination_class: Post)

    each_statement_mode do
      first_reference = DoubleAliasedRouteComment.new(route_fk: first.id, author_id: second.id)
      second_reference = DoubleAliasedRouteComment.new(route_fk: second.id, author_id: first.id)

      assert_equal first, first_reference.routed_post
      statement = reflection.association_scope_cache(Post, routes)
      assert_equal second, second_reference.routed_post
      assert_same statement, reflection.association_scope_cache(Post, routes)
    end
  end

  def test_cached_association_combines_owner_inputs_with_fixed_values
    first = Member.create!(member_type_id: 41)
    second = Member.create!(member_type_id: 42)
    first_target = Sponsor.create!(club_id: 41, sponsorable: first)
    second_target = Sponsor.create!(club_id: 42, sponsorable: second)
    Sponsor.create!(club_id: 42, sponsorable: first)
    Sponsor.create!(club_id: 41, sponsorable_id: first.id, sponsorable_type: Post.polymorphic_name)
    reflection = Member.reflect_on_association(:sponsor)

    with_constraints(reflection, reference_key: :club_id, target_key: :member_type_id) do
      routes = reflection.association_route_chain(origin_class: Member, destination_class: Sponsor)

      each_statement_mode do
        assert_equal first_target, first.reload_sponsor
        statement = reflection.association_scope_cache(Sponsor, routes)
        assert_equal second_target, second.reload_sponsor
        assert_same statement, reflection.association_scope_cache(Sponsor, routes)
      end
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

  def test_nil_association_predicate_preserves_scalar_and_composite_reference_shapes
    [
      [:post_id, :id, { "post_id" => nil }],
      [[:post_id], [:id], { "post_id" => nil }],
      [[:author_id, :post_id], [:author_id, :id], { "author_id" => nil, "post_id" => nil }],
    ].each do |foreign_key, primary_key, expected|
      reflection = ActiveRecord::Reflection.create(:belongs_to, :post, nil,
        { class_name: "Post", foreign_key: foreign_key, primary_key: primary_key }, NullableRouteComment)
      value = ActiveRecord::PredicateBuilder::AssociationQueryValue.new(reflection, nil, NullableRouteComment)

      assert_equal [expected], value.queries
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

  def test_normalized_single_column_match_preserves_array_valued_scalars
    skip unless current_adapter?(:PostgreSQLAdapter)

    record = ArrayRouteRecord.create!(big_int_data_points: [10, 20])
    route = ArrayRouteRecord.reflect_on_association(:same_array).association_route

    assert_not_predicate route.origin_key, :composite?
    assert_not_predicate route.destination_key, :composite?
    assert_equal [10, 20], route.origin_key.value_of(record)
    assert_equal [10, 20], route.destination_key.value_of(record)
    assert_equal record, record.same_array

    record.association(:same_array).reset
    ActiveRecord::Associations::Preloader.new(records: [record], associations: :same_array).call
    assert_predicate record.association(:same_array), :loaded?
    assert_equal record, record.same_array
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

  private
    def fresh_route_reflections
      [
        [ActiveRecord::Reflection.create(:belongs_to, :firm, nil,
          { class_name: "Firm", foreign_key: :client_of, inverse_of: false }, Client), Firm],
        [ActiveRecord::Reflection.create(:has_one, :sponsor, nil,
          { class_name: "Sponsor", as: :sponsorable, inverse_of: false }, Member), Sponsor],
        [ActiveRecord::Reflection.create(:belongs_to, :sponsorable, nil,
          { polymorphic: true, inverse_of: false }, Sponsor), Member],
      ]
    end

    def each_statement_mode(&)
      yield
      ActiveRecord::Base.lease_connection.unprepared_statement(&)
    end

    def build_mapping(reference_key:, target_key:)
      ActiveRecord::Key::Mapping.new(
        reference_key: ActiveRecord::Key.for(reference_key),
        target_key: ActiveRecord::Key.for(target_key)
      )
    end

    def with_constraints(reflection, reference_key:, target_key:, &block)
      constraints = build_mapping(reference_key: reference_key, target_key: target_key)
      reflection.clear_association_scope_cache unless reflection.polymorphic?
      reflection.stub(:association_route_constraints, constraints, &block)
    ensure
      reflection.clear_association_scope_cache unless reflection.polymorphic?
    end

    def build_belongs_to_route(reference:, constraints:)
      reference = build_mapping(**reference)
      constraints = build_mapping(**constraints)

      ActiveRecord::AssociationRoute.new(link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints))
    end

    def build_route(reference:, constraints:)
      reference = build_mapping(**reference)
      constraints = build_mapping(**constraints)

      ActiveRecord::AssociationRoute::Reverse.new(link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints))
    end
end
