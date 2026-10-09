# frozen_string_literal: true

require "cases/helper"
require "active_support/core_ext/object/with"
require "active_support/testing/ractors_assertions"
require "models/company"
require "models/member"
require "models/sponsor"
require "models/author"
require "models/comment"
require "models/post"
require "models/ship"

class AssociationRouteTest < ActiveRecord::TestCase
  include ActiveSupport::Testing::RactorsAssertions

  fixtures :posts

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

  class ConstrainedRouteMember < ActiveRecord::Base
    self.table_name = "members"

    has_one :routed_sponsor,
      -> { order(:id) },
      as: :sponsorable,
      class_name: "Sponsor"
  end

  class ArrayRouteRecord < ActiveRecord::Base
    self.table_name = "bigint_array"

    belongs_to :same_array,
      class_name: "AssociationRouteTest::ArrayRouteRecord",
      foreign_key: [:big_int_data_points],
      primary_key: [:big_int_data_points],
      optional: true
  end

  def test_constrained_routes_match_constraints_but_reference_only_the_reference
    link = ActiveRecord::AssociationLink.new(
      reference: build_mapping(reference_key: :post_id, target_key: :id),
      constraints: build_mapping(reference_key: :body, target_key: :author_id)
    )

    [ActiveRecord::AssociationRoute, ActiveRecord::AssociationRoute::Reverse].each do |route_class|
      route = route_class.new(link: link)
      expected_match = [["body", "author_id"], ["post_id", "id"]]
      expected_reference = [["post_id", "id"]]
      expected_constraint = [["body", "author_id"]]
      if route_class == ActiveRecord::AssociationRoute::Reverse
        expected_match.map!(&:reverse)
        expected_reference.map!(&:reverse)
        expected_constraint.map!(&:reverse)
      end

      assert_equal expected_match, route.each_match.to_a
      assert_equal expected_reference, route.each_reference.to_a
      assert_equal expected_constraint, route.each_constraint.to_a
    end
  end

  def test_routes_are_physically_oriented_from_the_foreign_key
    belongs_to = Client.reflect_on_association(:firm).association_route
    has_many = Firm.reflect_on_association(:clients_of_firm).association_route

    assert_equal [["client_of", "id"]], belongs_to.link.reference.to_a
    assert_equal belongs_to.link.reference, has_many.link.reference
    assert_equal ["client_of", "id"], [belongs_to.origin_key.name, belongs_to.destination_key.name]
    assert_equal ["id", "client_of"], [has_many.origin_key.name, has_many.destination_key.name]
  end

  if RUBY_VERSION >= "4.0"
    def test_shared_reflections_resolve_and_clear_routes_in_another_ractor
      fresh_route_reflections.each do |reflection, destination_class|
        destination_class.initialize_find_by_cache
        pairs = reflection.association_route(destination_class: destination_class).each_match.to_a
        ActiveSupport::Ractors.make_shareable(reflection)
        assert_ractor_shareable reflection

        resolved, rebuilt = on_ractor(reflection, destination_class) do |reflection, destination_class|
          before_clearing = reflection.association_route(destination_class: destination_class).each_match.to_a
          reflection.clear_association_scope_cache(destination_class)
          [before_clearing, reflection.association_route(destination_class: destination_class).each_match.to_a]
        end

        assert_equal pairs, resolved
        assert_equal pairs, rebuilt
      end
    end
  end

  def test_equivalent_inverse_polymorphic_routes_are_equal
    reflection = Member.reflect_on_association(:sponsor)
    origin_class = Class.new(Member) do
      def self.name = "EquivalentRouteMember"
    end
    variant = reflection.association_route(origin_class: origin_class)
    default = reflection.association_route

    assert_operator variant, :eql?, default
    assert_equal default.hash, variant.hash
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

  def test_query_constraints_filter_association_loads
    member = ConstrainedRouteMember.create!(member_type_id: 42)
    Sponsor.create!(club_id: 41, sponsorable: member)
    matching = Sponsor.create!(club_id: 42, sponsorable: member)
    reflection = ConstrainedRouteMember.reflect_on_association(:routed_sponsor)

    with_constraints(reflection, reference_key: :club_id, target_key: :member_type_id) do
      assert_equal matching, member.routed_sponsor
    end
  end

  def test_cached_association_loads_bind_each_owners_constraint_values
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

  def test_query_constraints_filter_disable_joins_loads
    author = Author.create!(name: "Constrained author")
    constrained_post = Post.create!(author: author, title: "Constrained post", body: "Post body")
    Comment.create!(post: constrained_post, body: "Not the post title")
    comment = Comment.create!(post: constrained_post, body: constrained_post.title)
    Comment.create!(post: constrained_post, body: "Not the post title either")

    Post.reflect_on_association(:comments).stub(:association_route, post_comments_route_constrained_by_title) do
      assert_equal [comment], author.no_joins_comments.to_a
    end
  end

  def test_association_predicates_match_records_completely_and_nil_by_reference
    reference_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "ConstrainedPredicateReference"

      belongs_to :routed_ship,
        class_name: "Ship",
        foreign_key: :club_id,
        primary_key: :pirate_id,
        optional: true
    end
    unset = reference_class.create!(club_id: nil, sponsorable_type: "Present constraint")
    reference_class.create!(club_id: 9_000_861, sponsorable_type: "Other constraint")
    reference = reference_class.create!(club_id: 9_000_861, sponsorable_type: "Present constraint")
    reference_class.create!(club_id: 9_000_861, sponsorable_type: "Another constraint")
    route = build_belongs_to_route(
      reference: { reference_key: :club_id, target_key: :pirate_id },
      constraints: { reference_key: :sponsorable_type, target_key: :name }
    )

    reference_class.reflect_on_association(:routed_ship).stub(:association_route, route) do
      assert_includes reference_class.where(routed_ship: nil), unset
      assert_includes reference_class.where(routed_ship: [nil]), unset

      target = Ship.new(pirate_id: reference.club_id, name: reference.sponsorable_type)
      assert_equal [reference], reference_class.where(routed_ship: target).to_a
      assert_equal [reference], reference_class.where(routed_ship: [target]).to_a
    end
  end

  def test_association_predicates_reject_bare_ids_with_query_constraints
    reflection = Comment.reflect_on_association(:post)
    route = build_belongs_to_route(
      reference: { reference_key: :post_id, target_key: :id },
      constraints: { reference_key: :body, target_key: :title }
    )

    reflection.stub(:association_route, route) do
      assert_raises(ArgumentError) { Comment.where(post: 1) }
      assert_raises(ArgumentError) { Comment.where(post: [nil, 1]) }
    end
  end

  def test_polymorphic_relation_predicates_match_query_constraints
    polymorphic_class = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"

      def self.name = "ConstrainedPolymorphicReference"

      belongs_to :subject,
        polymorphic: true,
        foreign_key: :club_id,
        foreign_type: :sponsorable_type,
        optional: true
    end
    post = Post.create!(title: "Polymorphic relation", body: "Polymorphic relation")
    polymorphic_class.create!(club_id: post.id, sponsor_type: "Not the post title", sponsorable_type: Post.polymorphic_name)
    polymorphic_reference = polymorphic_class.create!(club_id: post.id, sponsor_type: post.title, sponsorable_type: Post.polymorphic_name)
    polymorphic_class.create!(club_id: post.id, sponsor_type: "Not the post title either", sponsorable_type: Post.polymorphic_name)

    with_constraints(polymorphic_class.reflect_on_association(:subject), reference_key: :sponsor_type, target_key: :title) do
      assert_equal [polymorphic_reference], polymorphic_class.where(subject: post).to_a
      assert_equal [polymorphic_reference], polymorphic_class.where(subject: Post.where(id: post.id)).to_a
      assert_empty polymorphic_class.where(subject: Post.none)
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

    def post_comments_route_constrained_by_title
      build_route(
        reference: { reference_key: :post_id, target_key: :id },
        constraints: { reference_key: :body, target_key: :title }
      )
    end

    def build_route(reference:, constraints:)
      reference = build_mapping(**reference)
      constraints = build_mapping(**constraints)

      ActiveRecord::AssociationRoute::Reverse.new(link: ActiveRecord::AssociationLink.new(reference: reference, constraints: constraints))
    end
end
