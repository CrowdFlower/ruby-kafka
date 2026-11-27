# frozen_string_literal: true

module Kafka
  module Protocol
    class RequestMessage
      API_VERSION = 0

      def initialize(api_key:, api_version: API_VERSION, correlation_id:, client_id:, request:, flexible: false)
        @api_key = api_key
        @api_version = api_version
        @correlation_id = correlation_id
        @client_id = client_id
        @request = request
        @flexible = flexible
      end

      def encode(encoder)
        encoder.write_int16(@api_key)
        encoder.write_int16(@api_version)
        encoder.write_int32(@correlation_id)
        encoder.write_string(@client_id)
        # Flexible versions use a v2 request header, which differs from v1
        # only by this trailing tagged field buffer. The client id stays a
        # plain length-prefixed string.
        encoder.write_tagged_fields if @flexible

        @request.encode(encoder)
      end
    end
  end
end
