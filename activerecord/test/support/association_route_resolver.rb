# frozen_string_literal: true

class TestAssociationRouteResolver
  def initialize(route = nil, static: false, route_for: nil, route_for_target: nil,
    resolve_reference: nil, relation_route: nil)
    @route = route
    @static = static
    @route_for = route_for
    @route_for_target = route_for_target
    @resolve_reference = resolve_reference
    @relation_route = relation_route
  end

  def route_for(destination = nil)
    @route_for ? @route_for.call(destination) : @route
  end

  def route_for_target(target)
    @route_for_target ? @route_for_target.call(target) : @route
  end

  def resolve_reference(reference_record, &reader)
    @resolve_reference ? @resolve_reference.call(reference_record, reader) : @route
  end

  def resolve_reference_if_possible(reference_record, &reader)
    resolve_reference(reference_record, &reader)
  end

  def relation_route(target: nil)
    @relation_route ? @relation_route.call(target) : @route
  end

  def static?
    @static
  end

  def clear
  end
end
