# frozen_string_literal: true

require "optparse"

module Pgbus
  module CLI
    # Dead-letter queue management for headless deployments and incident
    # runbooks. Every operation routes through +Web::DataSource+ so the CLI
    # shares the dashboard's exact semantics — origin-queue re-enqueue,
    # transactional retry, lock release on discard, sensitive-payload
    # filtering — with zero raw SQL or direct PGMQ calls.
    module DLQ
      module_function

      ROW_FORMAT = "%-10s %-32s %-28s %-8s %-26s %s"
      REASON_WIDTH = 40

      def start(args, data_source: Pgbus::Web::DataSource.new)
        subcommand = args.first
        rest = args[1..] || []

        case subcommand
        when "list"       then list(rest, data_source)
        when "show"       then show(rest, data_source)
        when "retry"      then retry_one(rest, data_source)
        when "retry-all"  then retry_all(data_source)
        when "purge"      then purge(rest, data_source)
        else
          warn "Unknown dlq subcommand: #{subcommand.inspect}" if subcommand
          print_help
          exit 1
        end
      end

      def list(args, data_source)
        options = parse_list_options(args)
        messages = data_source.dlq_messages(page: options[:page], per_page: options[:per_page])

        if messages.empty?
          puts "No dead-letter messages."
          return
        end

        print_list_table(messages)
        puts "-" * 140
        puts "Total dead-letter messages: #{data_source.dlq_total_count} " \
             "(page #{options[:page]}, #{options[:per_page]} per page)"
      end

      def show(args, data_source)
        msg_id = require_msg_id(args)
        detail = data_source.dlq_message_detail(msg_id)
        abort_missing(msg_id) unless detail

        puts "msg_id:      #{detail[:msg_id]}"
        puts "dlq queue:   #{detail[:queue_name]}"
        puts "origin:      #{origin_queue(detail[:queue_name])}"
        puts "read_ct:     #{detail[:read_ct]}"
        puts "enqueued_at: #{detail[:enqueued_at]}"
        print_dead_letter(Pgbus::Web::DeadLetterReason.present(detail))
        puts "payload:"
        puts Pgbus::Web::PayloadFilter.filter_json(detail[:message])
      end

      def retry_one(args, data_source)
        msg_id = require_msg_id(args)
        detail = data_source.dlq_message_detail(msg_id)
        abort_missing(msg_id) unless detail

        if data_source.retry_dlq_message(detail[:queue_name], detail[:msg_id])
          puts "Re-enqueued msg_id #{detail[:msg_id]} to #{origin_queue(detail[:queue_name])}."
        else
          warn "Failed to retry msg_id #{detail[:msg_id]}."
          exit 1
        end
      end

      def retry_all(data_source)
        count = data_source.retry_all_dlq
        puts "Re-enqueued #{count} dead-letter message#{"s" unless count == 1}."
      end

      def purge(args, data_source)
        if args.include?("--all")
          purge_all(args, data_source)
        else
          purge_one(args, data_source)
        end
      end

      def purge_one(args, data_source)
        msg_id = require_msg_id(args)
        detail = data_source.dlq_message_detail(msg_id)
        abort_missing(msg_id) unless detail

        if data_source.discard_dlq_message(detail[:queue_name], detail[:msg_id])
          puts "Purged msg_id #{detail[:msg_id]} from #{detail[:queue_name]}."
        else
          warn "Failed to purge msg_id #{detail[:msg_id]}."
          exit 1
        end
      end

      def purge_all(args, data_source)
        unless args.include?("--yes")
          warn "Refusing to purge all dead-letter messages without --yes."
          exit 1
        end

        count = data_source.discard_all_dlq
        puts "Purged #{count} dead-letter message#{"s" unless count == 1}."
      end

      def parse_list_options(args)
        options = { page: 1, per_page: 25 }
        OptionParser.new do |opts|
          opts.banner = "Usage: pgbus dlq list [options]"
          opts.on("--page N", Integer, "Page number (default: 1)") { |v| options[:page] = v }
          opts.on("--per-page N", Integer, "Messages per page (default: 25)") { |v| options[:per_page] = v }
        end.parse!(args.dup)
        options
      end

      def print_list_table(messages)
        puts format(ROW_FORMAT, "MSG_ID", "DLQ QUEUE", "ORIGIN QUEUE", "READ_CT", "ENQUEUED_AT", "REASON")
        puts "-" * 140
        messages.each do |m|
          reason = Pgbus::Web::DeadLetterReason.present(m)
          puts format(ROW_FORMAT, m[:msg_id], m[:queue_name], origin_queue(m[:queue_name]),
                      m[:read_ct], m[:enqueued_at], (reason.error_class || reason.reason_key).to_s[0, REASON_WIDTH])
        end
      end

      # Why the message died, from its pgbus_dead_letter header (issue #495).
      def print_dead_letter(result)
        if result.legacy?
          puts "reason:      not recorded (dead-lettered before pgbus #{Pgbus::DeadLetterHeader::SINCE})"
          return
        end

        puts "died:        #{result.died_at}"
        puts "attempts:    #{result.attempts}/#{result.max_retries}"
        puts "source:      #{result.source_queue}"
        puts "reason:      #{result.reason}"
        puts "error:       #{dead_letter_error(result)}"
        puts "retried:     #{result.retried_before} time(s) from the DLQ before" if result.retried_before
      end

      def dead_letter_error(result)
        return "none recorded" unless result.error_class

        "#{result.error_class}: #{result.error_message} (attempt #{result.error_attempt})"
      end

      def origin_queue(dlq_queue_name)
        dlq_queue_name.delete_suffix(Pgbus::DEAD_LETTER_SUFFIX)
      end

      def require_msg_id(args)
        msg_id = args.find { |a| a !~ /\A--/ }
        unless msg_id
          warn "A msg_id is required."
          exit 1
        end
        msg_id
      end

      def abort_missing(msg_id)
        warn "No dead-letter message found with msg_id #{msg_id}."
        exit 1
      end

      def print_help
        puts <<~HELP
          Usage: pgbus dlq <subcommand> [options]

          Subcommands:
            list [--page N] [--per-page N]  List dead-letter messages
            show MSG_ID                     Show one message (payload filtered)
            retry MSG_ID                    Re-enqueue a message to its origin queue
            retry-all                       Re-enqueue every dead-letter message
            purge MSG_ID                    Discard a single message
            purge --all --yes               Discard every dead-letter message
        HELP
      end
    end
  end
end
