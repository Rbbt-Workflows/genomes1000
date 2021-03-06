require 'rbbt'
require 'rbbt/util/open'
require 'rbbt/resource'

module Genomes1000
  extend Resource
  self.subdir = "share/databases/genomes_1000"

  RELEASE_URL = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz"

  Genomes1000.claim Genomes1000.mutations, :proc do |filename|
    Open.write filename do |file|
      file.puts <<-EOF
#: :namespace=#{Genomes1000.organism}#:type=:flat
#RS ID\tGenomic Mutation
      EOF
      Open.read(RELEASE_URL) do |line|
        next if line[0] == "#"

        chr, pos, id, ref, alt, qual, filter, info = line.split("\t")
        pos, alt = Misc.correct_vcf_mutation(pos.to_i, ref, alt) 
        next if id == '.'

        mutation = [chr, pos, alt] * ":"
        file.puts [id, mutation] * "\t"
      end
    end
    nil
  end

  Genomes1000.claim Genomes1000.rsids, :proc do
    Workflow.require_workflow "Sequence"
    CMD.cmd('grep -v "^-"', :in => TSV.reorder_stream(Sequence::VCF.open_stream(Open.open(RELEASE_URL, :nocache => true), false, false, true), {0 => 2}), :pipe => true)
  end

  GM_SHARD_FUNCTION = Proc.new do |key|
    key[0..key.index(":")-1]
  end

  CHR_POS = Proc.new do |key|
    raise "Key (position) not String: #{ key }" unless String === key
    if match = key.match(/.*?:(\d+):?/)
      match[1].to_i
    else
      raise "Key (position) not understood: #{ key }"
    end
  end

  def self.database
    @@database ||= begin
                     Persist.persist_tsv("Genomes1000", Genomes1000.mutations, {}, :persist => true,
                                         :file => Rbbt.var.Genomes1000.mutations_shard.find,
                                         :prefix => "Genomes1000", :serializer => :string, :engine => "HDB",
                                         :shard_function => GM_SHARD_FUNCTION, :pos_function => CHR_POS) do |sharder|
                       sharder.fields = ["RS ID"]
                       sharder.key_field = "Genomic Position"
                       sharder.type = :single

                       TSV.traverse Genomes1000.mutations, :type => :array, :into => sharder, :bar => "Processing Genomes1000" do |line|
                         next if line =~ /^#/
                         rsid,_sep, mutation = line.partition "\t"
                         next if rsid == '.'
                         [mutation, rsid]
                       end
                      end
                    end
  end


  def self.rsid_database
    @@rsid_database ||= begin
                          Genomes1000.rsids.tsv :persist => true, :persist_file => Rbbt.var.Genomes1000.rsids.find
                        end
  end
end
