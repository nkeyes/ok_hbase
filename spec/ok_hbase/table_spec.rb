require 'spec_helper'

module OkHbase
  describe Table do
    let(:row_key1) { 'row1' }
    let(:row_key2) { 'row2' }
    let(:row_data1) { { 'd:foo' => 'Foo1', 'd:bar' => 'Bar1', 'd:baz' => 'Baz1' } }
    let(:row_data2) { { 'd:foo' => 'Foo2', 'd:bar' => 'Bar2', 'd:baz' => 'Baz2' } }
    let!(:timestamp) { (Time.now.to_f * 1000).to_i } # hbase timestamps are in milisecnds

    test_table_name = 'ok_hbase_test_table'
    conn = Connection.new(auto_connect: true, timeout: 60)

    before(:all) do
      conn.create_table(test_table_name, d: { max_versions: 3 })
    end

    after(:all) do
      conn.delete_table(test_table_name, true)
    end

    subject { Table.new(test_table_name, conn) }

    describe '#_scanner' do
      let(:scanner_opts) { {
          start_row: 'start',
          stop_row: 'stop',
          timestamp: Time.now,
          columns: ['d:foo', 'd:bar', 'i:baz'],
          caching: 1000,
          filter_string: 'filter',
      } }

      it 'should set all the options' do
        scanner = subject._scanner(scanner_opts)

        scanner.startRow.should == scanner_opts[:start_row]
        scanner.stopRow.should == scanner_opts[:stop_row]
        scanner.timestamp.should == scanner_opts[:timestamp]
        scanner.columns.should == scanner_opts[:columns]
        scanner.caching.should == scanner_opts[:caching]
        scanner.filterString.should == scanner_opts[:filter_string]
      end
    end

    describe '.scan' do

      it 'should convert a row prefix to a start and stop rows' do
        subject.put('scan_row1', row_data1)
        subject.put('scan_row2', row_data1)

        opts = { row_prefix: 'scan' }
        subject.should_receive(:_scanner).with(hash_including(start_row: 'scan', stop_row: 'scao')).and_return {
          Apache::Hadoop::Hbase::Thrift::TScan.new(
              startRow: 'scan',
              stopRow: 'scao',
              caching: 1000,
          )
        }

        subject.scan(opts) do |row_key, cols|
          cols.should == row_data1
        end
      end
    end

    describe '.regions' do
      it 'should list all region having rows for the table' do
        regions = subject.regions
        regions.should be_an Array
        regions.size.should be >= 1

        regions.each do |region|
          region.keys.should include('start_key', 'end_key', 'id', 'name', 'version')
        end

      end
    end

    describe 'CRUD' do

      describe '.put' do

        it 'should write to the row' do
          subject.put(row_key1, row_data1)

          subject.row(row_key1).should == row_data1
        end

        it 'should write to the row with a timestamp' do

          subject.put(row_key1, row_data1, timestamp)

          subject.row(row_key1, nil, nil, true).should == Hash[row_data1.map { |k, v| [k, [v, timestamp]] }]
        end
      end

      describe '.cells' do
        it 'should retrieve values' do

          subject.put(row_key2, row_data2.merge('d:foo' => 'OldFoo1'), timestamp-10)
          subject.put(row_key2, row_data2.merge('d:foo' => 'OldFoo2'), timestamp-5)
          subject.put(row_key2, row_data2, timestamp)

          subject.cells(row_key2, 'd:foo').should == ['Foo2', 'OldFoo2', 'OldFoo1']

          subject.cells(row_key2, 'd:foo', nil, timestamp-1).should == ['OldFoo2', 'OldFoo1']
          subject.cells(row_key2, 'd:foo', nil, timestamp-5).should == ['OldFoo1']

          subject.cells(row_key2, 'd:foo', nil, nil, true).should == [['Foo2', timestamp], ['OldFoo2', timestamp-5], ['OldFoo1', timestamp-10]]
          subject.cells(row_key2, 'd:foo', 2, nil, true).should == [['Foo2', timestamp], ['OldFoo2', timestamp-5]]
          subject.cells(row_key2, 'd:foo', 1, nil, true).should == [['Foo2', timestamp]]

          subject.cells(row_key2, 'd:foo', nil, nil, false).should == ['Foo2', 'OldFoo2', 'OldFoo1']
          subject.cells(row_key2, 'd:foo', 2, nil, false).should == ['Foo2', 'OldFoo2']
          subject.cells(row_key2, 'd:foo', 1, nil, false).should == ['Foo2']

        end
      end

      describe ".rows" do
        it 'should retrieve the specified rows' do

          subject.put(row_key1, row_data1, timestamp-10)
          subject.put(row_key2, row_data2.merge('d:foo' => 'OldFoo1'), timestamp-10)
          subject.put(row_key2, row_data2.merge('d:foo' => 'OldFoo2'), timestamp-5)
          subject.put(row_key2, row_data2, timestamp)

          subject.rows([row_key1, row_key2]).should == [[row_key1, row_data1], [row_key2, row_data2]]
          subject.rows([row_key1, row_key2], nil, timestamp-9).should == [[row_key1, row_data1], [row_key2, row_data2.merge('d:foo' => 'OldFoo1')]]

          subject.rows([row_key1, row_key2], row_data1.keys - ['d:foo']).should == [[row_key1, row_data1.except('d:foo')], [row_key2, row_data2.except('d:foo')]]
          subject.rows([row_key1, row_key2], row_data1.keys - ['d:foo'], timestamp-5).should == [[row_key1, row_data1.except('d:foo')], [row_key2, row_data2.except('d:foo')]]

        end
      end

      describe ".delete" do
        it 'should retrieve the specified rows' do
          subject.put(row_key1, row_data1)

          subject.delete(row_key1, ['d:bar', 'd:baz'])

          subject.row(row_key1).should == row_data1.except('d:bar', 'd:baz')
        end
      end
    end
  end
end
