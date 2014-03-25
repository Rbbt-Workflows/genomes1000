require 'rbbt'
require 'rbbt/workflow'

module Genomes1000
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"
end

require 'rbbt/sources/genomes1000'
require 'rbbt/sources/genomes1000/entity'
