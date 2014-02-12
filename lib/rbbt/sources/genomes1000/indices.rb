
module Genomes1000
  extend Workflow

  def self.rsid_index(organism, chromosome = nil)
    build = Organism.hg_build(organism)

    tag = [build, chromosome] * ":"
    Persist.persist("StaticPosIndex for Genomes1000 [#{ tag }]", :fwt, :persist => true) do
      value_size = 0
      file = Genomes1000[build == "hg19" ? "mutations" : "mutations_hg18"]
      chr_positions = []
      Open.read(CMD.cmd("grep '\t#{chromosome}:'", :in => file.open, :pipe => true)) do |line|
        next if line[0] == "#"[0]
        rsid, mutation = line.split("\t")
        next if mutation.nil? or mutation.empty?
        chr, pos = mutation.split(":")
        next if chr != chromosome or pos.nil? or pos.empty?
        chr_positions << [rsid, pos.to_i]
        value_size = rsid.length if rsid.length > value_size
      end
      fwt = FixWidthTable.new :memory, value_size
      fwt.add_point(chr_positions)
      fwt
    end
  end

  def self.mutation_index(organism)
    build = Organism.hg_build(organism)
    file = Genomes1000[build == "hg19" ? "mutations" : "mutations_hg18"]
    @mutation_index ||= {}
    @mutation_index[build] ||= file.tsv :persist => true, :fields => ["Genomic Mutation"], :type => :single, :persist => true
  end

end
