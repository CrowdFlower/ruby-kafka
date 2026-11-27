# frozen_string_literal: true

module Kafka
  module Protocol

    class ApiVersionsResponse
      class ApiInfo
        attr_reader :api_key, :min_version, :max_version

        def initialize(api_key:, min_version:, max_version:)
          @api_key, @min_version, @max_version = api_key, min_version, max_version
        end

        def api_name
          Protocol.api_name(api_key)
        end

        def version_supported?(version)
          (min_version..max_version).include?(version)
        end

        def to_s
          "#{api_name}=#{min_version}..#{max_version}"
        end

        def inspect
          "#<Kafka api version #{to_s}>"
        end
      end

      attr_reader :error_code, :apis

      def initialize(error_code:, apis:)
        @error_code = error_code
        @apis = apis
      end

      def self.decode(decoder)
        error_code = decoder.int16

        apis = decoder.array do
          ApiInfo.new(
            api_key: decoder.int16,
            min_version: decoder.int16,
            max_version: decoder.int16,
          )
        end

        new(error_code: error_code, apis: apis)
      end

      # @param api_version [Integer]
      # @return [#decode]
      def self.decoder_for(api_version)
        api_version >= FlexibleDecoder::FLEXIBLE_API_VERSION ? FlexibleDecoder : self
      end

      # Decodes the flexible form of the response. ApiVersions is the one
      # response that keeps a v0 header at every version, because the client
      # cannot know what the broker supports until this call returns.
      module FlexibleDecoder
        FLEXIBLE_API_VERSION = 3
        UNSUPPORTED_VERSION = 35

        def self.decode(decoder)
          error_code = decoder.int16

          # A broker that doesn't support this version replies with
          # UNSUPPORTED_VERSION and a v0 body, so there is nothing further
          # worth decoding. The caller retries at a version it will accept.
          return ApiVersionsResponse.new(error_code: error_code, apis: []) if error_code == UNSUPPORTED_VERSION

          apis = decoder.compact_array do
            api = ApiInfo.new(
              api_key: decoder.int16,
              min_version: decoder.int16,
              max_version: decoder.int16,
            )
            decoder.tagged_fields
            api
          end

          decoder.int32 # throttle_time_ms
          decoder.tagged_fields

          ApiVersionsResponse.new(error_code: error_code, apis: apis || [])
        end
      end
    end
  end
end
