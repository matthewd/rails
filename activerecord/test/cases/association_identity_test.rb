# frozen_string_literal: true

require "cases/helper"
require "active_support/core_ext/object/with"

class AssociationIdentityTest < ActiveRecord::TestCase
  class Target < ActiveRecord::Base
    self.table_name = "ships"
    default_scope -> { where(developer_id: 1) }, all_queries: true

    has_one :identity, class_name: "AssociationIdentityTest::Target", foreign_key: :id, inverse_of: false
    has_many :identities, class_name: "AssociationIdentityTest::Target", foreign_key: :id, inverse_of: false
  end

  class OtherTarget < ActiveRecord::Base
    self.table_name = "ships"
    default_scope -> { where(developer_id: 2) }, all_queries: true
  end

  class CounterReference < ActiveRecord::Base
    self.table_name = "sponsors"

    belongs_to :parent,
      class_name: "AssociationIdentityTest::Target",
      foreign_key: :club_id,
      primary_key: :pirate_id,
      query_constraints: { sponsor_type: :name },
      counter_cache: :treasures_count,
      touch: true,
      optional: true

    has_one :indirect_parent, through: :parent, source: :identity
    has_many :indirect_parents, through: :parent, source: :identities
  end

  class TouchReference < ActiveRecord::Base
    self.table_name = "sponsors"

    belongs_to :parent,
      class_name: "AssociationIdentityTest::Target",
      foreign_key: :club_id,
      primary_key: :pirate_id,
      query_constraints: { sponsor_type: :name },
      touch: true,
      optional: true
  end

  class PolymorphicReference < ActiveRecord::Base
    self.table_name = "sponsors"

    belongs_to :parent,
      polymorphic: true,
      foreign_key: :club_id,
      foreign_type: :sponsorable_type,
      primary_key: :pirate_id,
      query_constraints: { sponsor_type: :name },
      counter_cache: :treasures_count,
      touch: true,
      optional: true
  end

  class PolymorphicTouchReference < ActiveRecord::Base
    self.table_name = "sponsors"

    belongs_to :parent,
      polymorphic: true,
      foreign_key: :club_id,
      foreign_type: :sponsorable_type,
      primary_key: :pirate_id,
      query_constraints: { sponsor_type: :name },
      touch: true,
      optional: true
  end

  def test_constraint_only_changes_drive_current_and_saved_change_predicates
    first, second = create_targets
    reference = CounterReference.create!(club_id: first.pirate_id, sponsor_type: first.name)
    reference.reload
    association = reference.association(:parent)

    assert_not reference.parent_changed?
    assert_not reference.parent_previously_changed?
    assert_not association.saved_change_to_target?

    reference.sponsor_type = second.name

    assert reference.parent_changed?
    assert_not reference.parent_previously_changed?
    assert_not association.saved_change_to_target?
    assert_equal second, reference.parent

    reference.save!

    assert_not reference.parent_changed?
    assert reference.parent_previously_changed?
    assert association.saved_change_to_target?

    reference.save!

    assert_not reference.parent_previously_changed?
    assert_not association.saved_change_to_target?
  end

  def test_constraint_only_changes_migrate_counters_and_touch_both_targets
    [false, true].each do |loaded|
      first, second = create_targets(local_id: loaded ? 9_000_302 : 9_000_301)
      reference = CounterReference.create!(club_id: first.pirate_id, sponsor_type: first.name)
      assert_equal first, reference.parent if loaded
      assert_equal [1, 0], [first.reload.treasures_count, second.reload.treasures_count]
      reset_touch_times(first, second)

      reference.update!(sponsor_type: second.name)

      assert_equal [0, 1], [first.reload.treasures_count, second.reload.treasures_count]
      assert_operator first.updated_at, :>, original_time
      assert_operator second.updated_at, :>, original_time
      assert_equal second, reference.parent
      assert_equal first.pirate_id, reference.club_id

      reference.destroy!

      assert_equal [0, 0], [first.reload.treasures_count, second.reload.treasures_count]
    end
  end

  def test_constraint_only_changes_touch_both_targets_without_a_counter_cache
    [false, true].each do |loaded|
      first, second = create_targets(local_id: loaded ? 9_000_302 : 9_000_301)
      reference = TouchReference.create!(club_id: first.pirate_id, sponsor_type: first.name)
      assert_equal first, reference.parent if loaded
      reset_touch_times(first, second)

      reference.update!(sponsor_type: second.name)

      assert_operator first.reload.updated_at, :>, original_time
      assert_operator second.reload.updated_at, :>, original_time
      assert_equal second, reference.parent
    end
  end

  def test_counters_follow_constraints_when_the_assigned_target_does_not_match
    first, second = create_targets
    reference = CounterReference.new(sponsor_type: second.name)
    reference.parent = first

    reference.save!

    assert_equal second.name, reference.sponsor_type
    assert_equal [0, 1], [first.reload.treasures_count, second.reload.treasures_count]
    assert_equal second, reference.reload.parent
  end

  def test_touch_follows_constraints_when_the_assigned_target_does_not_match
    first, second = create_targets
    reference = TouchReference.new(sponsor_type: second.name)
    reference.parent = first
    reset_touch_times(first, second)

    reference.save!

    assert_equal original_time, first.reload.updated_at
    assert_operator second.reload.updated_at, :>, original_time
    assert_equal second, reference.reload.parent
  end

  def test_polymorphic_constraints_and_type_drive_change_tracking_and_counters
    first, second = create_targets
    other = OtherTarget.create!(pirate_id: first.pirate_id, name: second.name)
    reference = PolymorphicReference.create!(club_id: first.pirate_id,
      sponsor_type: first.name, sponsorable_type: Target.polymorphic_name)
    reference.reload
    assert_equal first, reference.parent

    reference.sponsor_type = second.name
    assert reference.parent_changed?
    reference.save!
    assert reference.parent_previously_changed?
    assert_equal [0, 1], [first.reload.treasures_count, second.reload.treasures_count]
    assert_equal second, reference.parent

    reset_touch_times(second, other)
    reference.sponsorable_type = OtherTarget.polymorphic_name
    assert reference.parent_changed?
    assert reference.association(:parent).stale_target?
    reference.save!

    assert reference.parent_previously_changed?
    assert reference.association(:parent).saved_change_to_target?
    assert_equal [0, 1], [second.reload.treasures_count, other.reload.treasures_count]
    assert_operator second.updated_at, :>, original_time
    assert_operator other.updated_at, :>, original_time
    assert_equal other, reference.parent
  end

  def test_polymorphic_type_only_changes_touch_both_targets_without_a_counter_cache
    first, = create_targets
    other = OtherTarget.create!(pirate_id: first.pirate_id, name: first.name)
    reference = PolymorphicTouchReference.create!(club_id: first.pirate_id,
      sponsor_type: first.name, sponsorable_type: Target.polymorphic_name)
    assert_equal first, reference.parent
    reset_touch_times(first, other)

    reference.update!(sponsorable_type: OtherTarget.polymorphic_name)

    assert_operator first.reload.updated_at, :>, original_time
    assert_operator other.reload.updated_at, :>, original_time
    assert_equal other, reference.parent
  end

  def test_nil_constraints_participate_in_counter_migration
    first = Target.create!(pirate_id: 9_000_301, name: nil)
    second = Target.create!(pirate_id: first.pirate_id, name: "New constraint")
    reference = CounterReference.create!(club_id: first.pirate_id)
    assert_equal first, reference.parent
    assert_equal [1, 0], [first.reload.treasures_count, second.reload.treasures_count]

    reference.update!(sponsor_type: second.name)

    assert_equal [0, 1], [first.reload.treasures_count, second.reload.treasures_count]
    assert_equal second, reference.parent
  end

  def test_stale_tracking_does_not_require_unselected_constraints_on_assignment
    first, = create_targets
    reference = CounterReference.instantiate("id" => 1, "club_id" => first.pirate_id)

    reference.parent = first

    assert_same first, reference.parent
    assert_not reference.association(:parent).stale_target?

    reference.sponsor_type = first.name

    assert reference.association(:parent).stale_target?
    assert reference.parent_changed?
    assert_equal first, reference.parent
  end

  def test_polymorphic_identity_tracking_does_not_resolve_the_target_class
    first, = create_targets
    reference = PolymorphicReference.instantiate("id" => 1, "club_id" => first.pirate_id,
      "sponsor_type" => first.name, "sponsorable_type" => Target.polymorphic_name)
    association = reference.association(:parent)
    association.target = first

    PolymorphicReference.stub(:polymorphic_class_for, ->(*) { raise "Unexpected class resolution" }) do
      reference.sponsor_type = "Different constraint"
      assert association.stale_target?
      assert reference.parent_changed?
      assert_not reference.parent_previously_changed?
      assert_not association.saved_change_to_target?

      reference.sponsor_type = first.name
      reference.sponsorable_type = "UnknownTarget"
      assert association.stale_target?
      assert reference.parent_changed?
    end
  end

  def test_aliased_constraint_and_polymorphic_type_drive_identity_tracking
    model = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"
      def self.name = "AliasedIdentityReference"
      alias_attribute :tenant, :sponsor_type
      alias_attribute :parent_type, :sponsorable_type

      belongs_to :parent, polymorphic: true,
        foreign_key: :club_id, foreign_type: :parent_type, primary_key: :pirate_id,
        query_constraints: { tenant: :name }, optional: true
    end
    first, second = create_targets
    reference = model.create!(club_id: first.pirate_id, tenant: first.name, parent_type: Target.polymorphic_name)
    reference.reload
    assert_not reference.parent_changed?
    assert_equal first, reference.parent

    reference.tenant = second.name
    assert reference.parent_changed?
    reference.save!
    assert reference.parent_previously_changed?
    assert reference.association(:parent).saved_change_to_target?
    assert_equal second, reference.parent

    reference.parent_type = OtherTarget.polymorphic_name
    assert reference.parent_changed?
    assert reference.association(:parent).stale_target?
    assert_nil reference.parent
  end

  def test_touch_can_replace_an_unresolvable_old_polymorphic_type
    first, = create_targets
    ["MissingTouchedClass", "MissingTouchNamespace::MissingTarget"].each do |old_type|
      reference = PolymorphicTouchReference.create!
      reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name, sponsorable_type: old_type)
      reset_touch_times(first)

      reference.update!(sponsorable_type: Target.polymorphic_name)

      assert_equal first, reference.parent
      assert_operator first.reload.updated_at, :>, original_time
    end
  end

  def test_touch_still_rejects_an_unresolvable_new_polymorphic_type
    first, = create_targets
    reference = PolymorphicTouchReference.create!(club_id: first.pirate_id,
      sponsor_type: first.name, sponsorable_type: Target.polymorphic_name)
    reset_touch_times(first)

    assert_raises(NameError) { reference.update!(sponsorable_type: "MissingNewTarget") }

    assert_equal Target.polymorphic_name, reference.reload.sponsorable_type
    assert_equal original_time, first.reload.updated_at
  end

  def test_touch_can_replace_an_unresolvable_old_demodulized_polymorphic_type
    model = Class.new(PolymorphicTouchReference) do
      self.store_full_class_name = false
      def self.name = "AssociationIdentityTest::ScopedTouchReference"
    end
    first, = create_targets
    reference = model.create!
    reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name, sponsorable_type: "MissingTouchedClass")
    reset_touch_times(first)

    reference.update!(sponsorable_type: "Target")

    assert_equal first, reference.parent
    assert_operator first.reload.updated_at, :>, original_time
  end

  def test_touch_does_not_hide_unrelated_name_errors_from_polymorphic_resolution
    model = Class.new(PolymorphicTouchReference) do
      def self.name = "BrokenTouchReference"
      def self.polymorphic_class_for(name)
        raise NameError.new("uninitialized constant InternalDependency", :InternalDependency) if name == "OldTarget"
        super
      end
    end
    first, = create_targets
    reference = model.create!
    reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name, sponsorable_type: "OldTarget")

    error = assert_raises(NameError) { reference.update!(sponsorable_type: Target.polymorphic_name) }

    assert_equal :InternalDependency, error.name
    assert_equal "OldTarget", reference.reload.sponsorable_type
  end

  def test_touch_does_not_hide_no_method_errors_from_polymorphic_resolution
    model = Class.new(PolymorphicTouchReference) do
      def self.name = "BrokenTouchReference"
      def self.polymorphic_class_for(name)
        raise NoMethodError.new("undefined method MissingMethod", :MissingMethod) if name == "MissingMethod"
        super
      end
    end
    first, = create_targets
    reference = model.create!
    reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name, sponsorable_type: "MissingMethod")

    assert_raises(NoMethodError) { reference.update!(sponsorable_type: Target.polymorphic_name) }

    assert_equal "MissingMethod", reference.reload.sponsorable_type
  end

  def test_constraint_changes_invalidate_through_associations
    first, second = create_targets
    reference = CounterReference.create!(club_id: first.pirate_id, sponsor_type: first.name)
    assert_equal first, reference.indirect_parent
    assert_equal [first], reference.indirect_parents.to_a

    reference.sponsor_type = second.name

    assert reference.association(:indirect_parent).stale_target?
    assert reference.association(:indirect_parents).stale_target?
    assert_equal second, reference.indirect_parent
    assert_equal [second], reference.indirect_parents.to_a
  end

  def test_constraint_changes_invalidate_negative_through_caches
    first, = create_targets
    reference = CounterReference.create!(club_id: first.pirate_id, sponsor_type: "No match")
    assert_nil reference.indirect_parent
    assert_empty reference.indirect_parents.to_a

    reference.sponsor_type = first.name

    assert reference.association(:indirect_parent).stale_target?
    assert reference.association(:indirect_parents).stale_target?
    assert_equal first, reference.indirect_parent
    assert_equal [first], reference.indirect_parents.to_a
  end

  def test_touch_does_not_invent_an_old_target_when_the_old_type_was_nil
    first, = create_targets
    second = Target.create!(pirate_id: first.pirate_id + 1, name: first.name)
    reference = PolymorphicTouchReference.create!
    reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name)
    reset_touch_times(first, second)

    reference.update!(club_id: second.pirate_id, sponsorable_type: Target.polymorphic_name)

    assert_equal original_time, first.reload.updated_at
    assert_operator second.reload.updated_at, :>, original_time
    assert_equal second, reference.parent
  end

  def test_counter_migration_has_no_old_target_when_the_old_type_was_nil
    first, = create_targets
    reference = PolymorphicReference.create!
    reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name)

    reference.update!(sponsorable_type: Target.polymorphic_name)

    assert_equal 1, first.reload.treasures_count
    assert_equal first, reference.parent
  end

  def test_touch_with_a_pending_constraint_change_touches_both_targets
    model = Class.new(ActiveRecord::Base) do
      self.table_name = "comments"
      self.inheritance_column = nil
      def self.name = "PendingIdentityReference"

      belongs_to :parent, class_name: "AssociationIdentityTest::Target",
        foreign_key: :author_id, primary_key: :pirate_id,
        query_constraints: { body: :name }, touch: true, optional: true
    end
    first, second = create_targets
    reference = model.create!(post_id: -1, author_id: first.pirate_id, body: first.name)
    assert_equal first, reference.parent
    reset_touch_times(first, second)

    reference.body = second.name
    reference.touch

    assert_operator first.reload.updated_at, :>, original_time
    assert_operator second.reload.updated_at, :>, original_time
    assert_equal second, reference.parent
  end

  def test_counter_migration_does_not_ignore_an_unresolvable_old_polymorphic_type
    first, = create_targets
    reference = PolymorphicReference.create!
    reference.update_columns(club_id: first.pirate_id, sponsor_type: first.name, sponsorable_type: "MissingCountedClass")

    assert_raises(NameError) { reference.update!(sponsorable_type: Target.polymorphic_name) }

    assert_equal 0, first.reload.treasures_count
    assert_equal "MissingCountedClass", reference.reload.sponsorable_type
  end

  def test_conditional_presence_validation_checks_constraint_changes
    ActiveRecord.with(belongs_to_required_validates_foreign_key: false) do
      model = Class.new(ActiveRecord::Base) do
        self.table_name = "sponsors"
        def self.name = "RequiredIdentityReference"

        belongs_to :parent,
          class_name: "AssociationIdentityTest::Target",
          foreign_key: :club_id,
          primary_key: :pirate_id,
          query_constraints: { sponsor_type: :name },
          optional: false
      end
      first, = create_targets
      reference = model.create!(club_id: first.pirate_id, sponsor_type: first.name)
      reference.reload

      assert_no_queries { assert reference.valid? }
      assert_equal first, reference.parent

      reference.sponsor_type = "Unmatched constraint"

      assert_not reference.valid?
      assert_includes reference.errors[:parent], "must exist"
      assert_equal first.pirate_id, reference.club_id
    end
  end

  def test_conditional_polymorphic_presence_validation_checks_type_changes
    ActiveRecord.with(belongs_to_required_validates_foreign_key: false) do
      model = Class.new(ActiveRecord::Base) do
        self.table_name = "sponsors"
        def self.name = "RequiredPolymorphicIdentityReference"

        belongs_to :parent, polymorphic: true,
          foreign_key: :club_id, foreign_type: :sponsorable_type, primary_key: :pirate_id,
          query_constraints: { sponsor_type: :name }, optional: false
      end
      first, = create_targets
      reference = model.create!(club_id: first.pirate_id, sponsor_type: first.name,
        sponsorable_type: Target.polymorphic_name)
      reference.reload
      assert_no_queries { assert reference.valid? }
      assert_equal first, reference.parent

      reference.sponsorable_type = OtherTarget.polymorphic_name

      assert_not reference.valid?
      assert_includes reference.errors[:parent], "must exist"
    end
  end

  def test_assigned_unsaved_targets_are_preserved_without_a_reference
    target = Target.new(name: "New target")
    reference = CounterReference.new(sponsor_type: target.name)
    reference.parent = target
    reference.sponsor_type = "Different constraint"

    assert_same target, reference.parent
    assert reference.parent_changed?

    polymorphic = PolymorphicReference.new(sponsor_type: target.name)
    polymorphic.parent = target
    polymorphic.sponsor_type = "Different constraint"
    polymorphic.sponsorable_type = OtherTarget.polymorphic_name

    assert_same target, polymorphic.parent
    assert polymorphic.parent_changed?
  end

  def test_composite_references_and_constraints_drive_identity_tracking
    model = Class.new(ActiveRecord::Base) do
      self.table_name = "sponsors"
      def self.name = "CompositeIdentityReference"

      belongs_to :parent, class_name: "AssociationIdentityTest::Target",
        foreign_key: [:club_id, :sponsorable_id], primary_key: [:pirate_id, :id],
        query_constraints: { sponsor_type: :name }, counter_cache: :treasures_count, optional: true
    end
    first, second = create_targets
    reference = model.create!(club_id: first.pirate_id, sponsorable_id: first.id, sponsor_type: first.name)
    assert_equal first, reference.parent

    reference.sponsor_type = second.name
    assert reference.parent_changed?
    assert reference.association(:parent).stale_target?
    reference.save!

    assert reference.parent_previously_changed?
    assert_nil reference.parent
    assert_equal [0, 0], [first.reload.treasures_count, second.reload.treasures_count]

    reference.sponsorable_id = nil
    reference.save!
    reference.sponsor_type = first.name
    reference.save!

    assert_nil reference.parent
    assert_equal [0, 0], [first.reload.treasures_count, second.reload.treasures_count]
    assert reference.parent_previously_changed?
  end

  private
    def create_targets(local_id: 9_000_301)
      [Target.create!(pirate_id: local_id, name: "First constraint"),
       Target.create!(pirate_id: local_id, name: "Second constraint")]
    end

    def original_time
      Time.utc(2000)
    end

    def reset_touch_times(*targets)
      targets.each { |record| record.update_columns(updated_at: original_time) }
    end
end
