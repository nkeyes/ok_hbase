require 'spec_helper'

module OkHbase
  describe Table do
    test_table_name = 'ok-hbase_test_table'
    conn = Connection.new(auto_connect: true, timeout: 60)

    before(:all) do
      conn.create_table(test_table_name, d: {})
    end

    after(:all) do
      conn.delete_table(test_table_name, true)
    end

    subject { Table.new(test_table_name, conn) }

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

        subject.scan(opts) {}
      end
    end

    describe ".regions" do
      it "should list all region having rows for the table" do
        regions = subject.regions
        regions.should be_an Array
        regions.size.should be >= 1

        regions.each do |region|
          region.keys.should include('start_key', 'end_key', 'id', 'name', 'version')
        end

      end
    end
  end
end
