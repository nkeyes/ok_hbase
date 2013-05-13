require 'spec_helper'

module OkHbase
  describe Connection do
    describe ".create_table" do
      let(:conn) { Connection.new auto_connect: true, timeout: 60 }

      it "should create tables with the right column families" do
        name = "ok-hbase_test_table"
        column_families = {
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

        expected_families = Hash[column_families.map { |cf, data| [cf, { 'name' => "#{cf}:" }.merge(data)] }]

        #sanity check
        conn.tables.should_not include(name)

        conn.create_table(name, column_families)

        conn.tables.should include(name)

        table = conn.table(name)

        table.families.should == expected_families

        #cleanup
        conn.delete_table(name, true)
      end


      it "should create tables with the right name" do
        name = "ok-hbase_test_table"
        column_families = {
            'd' => {}
        }

        #sanity check
        conn.tables.should_not include(name)

        conn.create_table(name, column_families)

        conn.tables.should include(name)

        #cleanup
        conn.delete_table(name, true)
      end
    end

    describe ".open" do
      let(:conn) { Connection.new }

      it "should open a connection" do
        expect { conn.open }.to change { conn.open? }.to(true)
      end
    end

    describe ".close" do
      let(:conn) { Connection.new auto_connect: true }

      it "should close a connection" do
        expect { conn.close }.to change { conn.open? }.to(false)
      end
    end

    describe ".tables" do
      let(:conn) { Connection.new auto_connect: true }

      it "should return an array of table names" do
        conn.tables.should be_an Array
      end
    end
  end
end
