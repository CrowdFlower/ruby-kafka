# frozen_string_literal: true

module Kafka
  module Protocol

    # Asks the broker which API versions it supports, and tells it which
    # client is asking.
    #
    # ## API Specification
    #
    #     ApiVersionsRequest (Version: 3) => client_software_name client_software_version TAG_BUFFER
    #         client_software_name => COMPACT_STRING
    #         client_software_version => COMPACT_STRING
    #
    # The two client_software fields were added by KIP-511. Without them the
    # broker has no way to identify the client, and Confluent Cloud reports it
    # as an unknown version. Versions below 3 have an empty body.
    class ApiVersionsRequest
      API_VERSION = 3
      FLEXIBLE_API_VERSION = 3

      CLIENT_SOFTWARE_NAME = "ruby-kafka"

      attr_reader :api_version

      def initialize(api_version: API_VERSION)
        @api_version = api_version
      end

      def api_key
        API_VERSIONS_API
      end

      def flexible?
        api_version >= FLEXIBLE_API_VERSION
      end

      def encode(encoder)
        return unless flexible?

        encoder.write_compact_string(CLIENT_SOFTWARE_NAME)
        encoder.write_compact_string(client_software_version)
        encoder.write_tagged_fields
      end

      def response_class
        ApiVersionsResponse.decoder_for(api_version)
      end

      private

      # KIP-511 restricts this to [A-Za-z0-9.-]+, and rejects an empty value.
      def client_software_version
        version = Kafka::VERSION.gsub(/[^A-Za-z0-9.-]/, "-")

        version.empty? ? "unknown" : version
      end
    end

  end
end
