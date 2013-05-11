require 'spec_helper'

module OkHbase
  describe Connection do
    describe ".open" do
      let(:conn) { Connection.new }

      it "should open a connection" do
        expect { conn.open }.to change { conn.open? }.to(true)
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
