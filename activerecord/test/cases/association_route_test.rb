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

end
