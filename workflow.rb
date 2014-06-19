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
    TSV.traverse mutations, :into => dumper, :bar => true do |mutation|
      next if mutation.empty?
      rsid = database[mutation]
      next if rsid.nil?
      [mutation, rsid]
    end
  end
end

require 'rbbt/sources/genomes1000'
require 'rbbt/sources/genomes1000/entity'
