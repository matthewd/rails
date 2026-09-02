# frozen_string_literal: true

module ActiveRecord::Associations
  module ForeignAssociation # :nodoc:
    def foreign_key_present?
      if reflection.klass.primary_key
        association_route.link.reference.target_key.all? do |key|
          owner.attribute_present?(key)
        end
      else
        false
      end
    end

    def nullified_owner_attributes
      route = association_route
      Hash.new.tap do |attrs|
        route.link.reference.reference_key.each { |foreign_key| attrs[foreign_key] = nil }
        route.fixed_reference_values.each_key { |column| attrs[column] = nil }
      end
    end

    private
      # Sets the owner attributes on the given record
      def set_owner_attributes(record)
        return if options[:through]

        association_route.write(owner, record)
      end

      def association_route
        reflection.association_route_for_target(owner)
      end
  end
end
