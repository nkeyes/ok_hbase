require 'spec_helper'

describe OkHbase do

  describe "#increment_string" do
    let(:normal_string) { "foo baq" }
    let(:incremented_normal_string) { "foo bar" }

    let(:binary_string) { [1, 2, 3, 255].pack('c*') }
    let(:incremented_binary_string) { [1, 2, 4].pack('c*') }
    let(:max_binary_string) { [255, 255, 255].pack('c*') }

    it "should increment the last byte byte < 255" do
      OkHbase::increment_string(normal_string).should == incremented_normal_string
      OkHbase::increment_string(binary_string).should == incremented_binary_string
    end

    it "should return nil when all bytes are 255" do
      OkHbase::increment_string(max_binary_string).should be_nil
    end

  end
end
