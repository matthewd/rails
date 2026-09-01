# frozen_string_literal: true

require "cases/helper"
require "models/company"
require "models/member"
require "models/sponsor"
require "models/author"
require "models/person"
require "models/comment"

class AssociationRouteTest < ActiveRecord::TestCase
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
end
