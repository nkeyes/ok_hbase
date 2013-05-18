# -*- encoding: utf-8 -*-
# require 'spec_helper'

describe OkHbase do

  describe "#increment_string" do
    let(:normal_string) { "foo baq" }
    let(:incremented_normal_string) { "foo bar" }

    let(:utf8_string) { "Vulgar fraction one half: ¼".force_encoding(Encoding::UTF_8) }
    let(:incremented_utf8_string) { "Vulgar fraction one half: ½".force_encoding(Encoding::UTF_8) }
    let(:max_byte_string) { [255, 255, 255].pack('c*') }

    it "should increment the last byte byte < 255" do
      OkHbase::increment_string(normal_string).should == incremented_normal_string
      OkHbase::increment_string(utf8_string).should == incremented_utf8_string
    end

    it "should return nil when all bytes are 255" do
      OkHbase::increment_string(max_byte_string).should be_nil
    end

  end
end
