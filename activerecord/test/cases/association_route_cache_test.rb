# frozen_string_literal: true

require "active_record"
require "active_support/test_case"
require "active_support/testing/ractors_assertions"
require "weakref"

class AssociationRouteCacheTest < ActiveSupport::TestCase
  include ActiveSupport::Testing::RactorsAssertions

  def test_discarded_reflections_release_their_routes_and_models
    refs = Thread.new do
      reflection, owner, destination = build_reflection
      origin = Class.new(owner)
      route = reflection.association_route(origin_class: origin, destination_class: destination)
      weak_references(reflection, owner, origin, destination, route)
    end.value

    assert_collected refs
  end

  def test_discarded_reflections_release_default_routes
    refs = Thread.new do
      reflection, owner, destination = build_reflection
      route = reflection.association_route
      weak_references(reflection, owner, destination, route)
    end.value

    assert_collected refs
  end

  def test_live_reflections_do_not_retain_resolved_endpoint_classes
    reflection, owner, destination = build_reflection
    refs = Thread.new do
      origin = Class.new(owner)
      target = Class.new(destination)
      reflection.association_route(origin_class: origin, destination_class: target)
      weak_references(origin, target)
    end.value

    assert_collected refs
    assert_equal [["id", "owner_id"]], reflection.association_route.each_match.to_a
  end

  def test_dup_and_clear_do_not_share_reflection_caches
    reflection, _, destination = build_reflection
    copy = reflection.dup
    original_route = reflection.association_route
    copied_route = copy.association_route

    assert_equal original_route, copied_route
    assert_not_same original_route, copied_route

    reflection.clear_association_scope_cache(destination)

    assert_not_same original_route, reflection.association_route
    assert_same copied_route, copy.association_route
  end

  def test_clear_removes_the_owner_entry
    reflection, _, destination = build_reflection
    reflection.association_route
    registry = ActiveSupport::Ractors[:active_record_association_route_caches]
    assert registry.key?(reflection)

    reflection.clear_association_scope_cache(destination)

    assert_not registry.key?(reflection)
  end

  def test_live_owners_preserve_warm_values_across_gc
    reflection, owner, destination = build_reflection
    origin = Class.new(owner)
    route = reflection.association_route(origin_class: origin, destination_class: destination)
    default_route = reflection.association_route

    collect

    assert_same route, reflection.association_route(origin_class: origin, destination_class: destination)
    assert_same default_route, reflection.association_route
  end

  def test_threads_share_warm_values
    reflection, _, _ = build_reflection
    cache = ActiveRecord::AssociationRoute::Cache.for(reflection)
    reflection.association_route
    routes = 4.times.map { Thread.new { reflection.association_route } }.map(&:value)

    routes.each { |route| assert_same routes.first, route }
    assert_same routes.first, cache.fetch_or_store(nil) { flunk "The default route should be cached" }
  end

  def test_overlapping_route_construction_produces_equivalent_routes
    reflection, _, _ = build_reflection
    build = reflection.method(:build_default_association_route)
    entered, release = Queue.new, Queue.new
    reflection.define_singleton_method(:build_default_association_route) do
      entered << true
      release.pop
      build.call
    end
    threads = Array.new(4) { Thread.new { reflection.association_route } }
    4.times { entered.pop }
    4.times { release << true }
    routes = threads.map(&:value)

    routes.each { |route| assert_equal routes.first, route }
    assert_equal [["id", "owner_id"]], routes.first.each_match.to_a
    assert_equal routes.first, reflection.association_route
  end

  if RUBY_VERSION >= "4.0"
    def test_shared_reflections_and_routes_keep_ractor_local_caches
      reflection, owner, destination = build_reflection
      route = reflection.association_route
      ActiveSupport::Ractors.make_shareable(reflection)
      ActiveSupport::Ractors.make_shareable(route)
      assert_ractor_shareable reflection
      assert_ractor_shareable route

      pairs, rebuilt = on_ractor(reflection, route, owner, destination) do |reflection, route, owner, destination|
        before = reflection.association_route
        reflection.clear_association_scope_cache(destination)
        [before.each_match.to_a, !before.equal?(reflection.association_route)]
      end

      assert_equal [["id", "owner_id"]], pairs
      assert rebuilt
      assert_same route, reflection.association_route
    end
  end

  private
    def build_model
      Class.new(ActiveRecord::Base) do
        self.primary_key = "id"
        def self.name = "CacheRouteModel"
        def self.type_for_attribute(_name) = ActiveModel::Type::Integer.new
      end
    end

    def build_reflection
      owner, destination = build_model, build_model
      reflection = ActiveRecord::Reflection::HasManyReflection.new(:children, nil,
        { anonymous_class: destination, foreign_key: "owner_id", primary_key: "id", inverse_of: false }, owner)
      owner._reflections = { children: reflection }.freeze
      [reflection, owner, destination]
    end

    def weak_references(*objects)
      objects.map { |object| WeakRef.new(object) }
    end

    def assert_collected(refs)
      collect
      refs.each { |ref| assert_not ref.weakref_alive? }
    end

    def collect
      4.times { GC.start(full_mark: true, immediate_sweep: true) }
    end
end
