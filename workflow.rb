require 'rbbt'
require 'rbbt/workflow'

module Genomes1000
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  input :mutations, :array, "Genomic Mutation"
  task :identify => :tsv do |mutations|
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"], :type => :single
    dumper.init
    database = Genomes1000.database
    TSV.traverse mutations, :into => dumper, :bar => "Genomes1000", :type => :array do |mutation|
      next if mutation.empty?
      rsid = database[mutation]
      next if rsid.nil?
      [mutation, rsid]
    end
  end

  dep :identify
  task :annotate => :tsv do 
    database = Genomes1000.rsid_database
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"] + database.fields[1..-1], :type => :single
    dumper.init
    TSV.traverse step(:identify), :into => dumper, :bar => "Genomes1000 annotate" do |mutation, rsid|
      next if mutation.empty?
      values = database[rsid]
      next if values.nil?
      values[0] = rsid
      [mutation, values]
    end
  end
end

require 'rbbt/sources/genomes1000'
require 'rbbt/sources/genomes1000/entity'
