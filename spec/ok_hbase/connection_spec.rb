require 'spec_helper'

module OkHbase
  describe Connection do
    let(:connect_options) { {} }
    let(:conn) { Connection.new(connect_options) }

    let(:test_table_name) { 'ok_hbase_test_table' }
    let(:test_table_column_families) {
      {
          'a' => {
              'max_versions' => 5,
              'compression' => 'GZ',
              'in_memory' => true,
              'bloom_filter_type' => 'ROW',
              'block_cache_enabled' => true,

              #TODO find out if these aren't being set properly or just aren't reported properly. 0 is the default
              'bloom_filter_vector_size' => 0,
              'bloom_filter_nb_hashes' => 0,

              #TODO find out why this doesn't get reported properly. -1 is the default
              'time_to_live' => -1
          },
          'b' => {
              'max_versions' => 15,
              'compression' => 'NONE',
              'in_memory' => true,
              'bloom_filter_type' => 'ROWCOL',
              'block_cache_enabled' => true,

              'bloom_filter_vector_size' => 0,
              'bloom_filter_nb_hashes' => 0,

              'time_to_live' => -1
          }
      }
    }

    let(:test_table) {
      conn.table(test_table_name) || conn.create_table(test_table_name, test_table_column_families)
    }


    describe '#create_table' do
      before { connect_options[:timeout] = 60 }
      after { conn.delete_table(test_table_name, true) }

      it 'should create tables with the right column families' do

        expected_families = Hash[test_table_column_families.map { |cf, data| [cf, { 'name' => "#{cf}:" }.merge(data)] }]

        #sanity check
        conn.tables.should_not include(test_table_name)

        conn.create_table(test_table_name, test_table_column_families)

        conn.tables.should include(test_table_name)

        table = conn.table(test_table_name)

        table.families.should == expected_families

      end


      it 'should create tables with the right name' do
        name = 'ok_hbase_test_table'
        column_families = {
            'd' => {}
        }

        #sanity check
        conn.tables.should_not include(name)

        conn.create_table(name, column_families)

        conn.tables.should include(name)
      end
    end

    describe '#open' do
      before { connect_options[:auto_connect] = false }

      it 'should open a connection' do
        expect { conn.open }.to change { conn.open? }.to(true)
      end
    end

    describe '#close' do
      before { connect_options[:auto_connect] = true }

      it 'should close a connection' do
        expect { conn.close }.to change { conn.open? }.to(false)
      end
    end

    describe '#tables' do
      before { connect_options[:auto_connect] = true }

      it 'should return an array of table names' do
        conn.tables.should be_an Array
      end
    end

    describe '#increment' do
      before do
        connect_options[:auto_connect] = true
        conn.create_table(test_table_name, test_table_column_families)

        test_table.put('test_row', { 'a:test_column' => [0].pack('Q>*') })
      end

      after { conn.delete_table(test_table_name, true) }

      it 'should increment the right cell by the expected amount' do
        expect {
          conn.increment(table: test_table_name, row: 'test_row', column: 'a:test_column', amount: 2)
        }.to change { test_table.row('test_row')['a:test_column'] }.to([2].pack('Q>*'))
      end
    end

    describe '#increment_rows' do
      before do
        connect_options[:auto_connect] = true
        conn.create_table(test_table_name, test_table_column_families)

        test_table.put('test_row1', { 'a:test_column' => [0].pack('Q>*') })
        test_table.put('test_row2', { 'a:test_column' => [1].pack('Q>*') })
      end

      after { conn.delete_table(test_table_name, true) }

      it 'should increment the right cells by the expected amounts' do
        expect {
          conn.increment_rows([{ table: test_table_name, row: 'test_row1', column: 'a:test_column', amount: 2 },
                          { table: test_table_name, row: 'test_row2', column: 'a:test_column', amount: 3 }])
        }.to change { [test_table.row('test_row1')['a:test_column'], test_table.row('test_row2')['a:test_column']] }.to([[2].pack('Q>*'), [4].pack('Q>*')])
      end
    end
  end
end
