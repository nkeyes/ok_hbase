require 'spec_helper'

module OkHbase
  describe Table do
    subject { Table.new('nkeyes_test_subspace_messages', Connection.new(auto_connect: true)) }

    describe "#_scanner" do
      let(:scanner_opts) { {
          start_row: 'start',
          stop_row: 'stop',
          timestamp: Time.now,
          columns: ['d:foo', 'd:bar', 'i:baz'],
          caching: 1000,
          filter_string: 'filter',
      } }

      it "should set all the options" do
        scanner = subject.class.send(:_scanner, scanner_opts)

        scanner.startRow.should == scanner_opts[:start_row]
        scanner.stopRow.should == scanner_opts[:stop_row]
        scanner.timestamp.should == scanner_opts[:timestamp]
        scanner.columns.should == scanner_opts[:columns]
        scanner.caching.should == scanner_opts[:caching]
        scanner.filterString.should == scanner_opts[:filter_string]
      end
    end

    describe ".scan" do

      it "shoud convert a row prefix to a start and stop rows" do

        opts = { row_prefix: 'aaa' }
        subject.class.should_receive(:_scanner).with(hash_including(start_row: 'aaa', stop_row: 'aab')).and_return {
          Apache::Hadoop::Hbase::Thrift::TScan.new(
              startRow: 'aaa',
              stopRow: 'aab',
              caching: 1000,
          )
        }

        subject.scan opts do

        end

      end

    end
  end
end
