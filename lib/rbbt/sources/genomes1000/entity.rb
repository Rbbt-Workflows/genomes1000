require 'rbbt/sources/genomes1000/indices'

if defined? Entity
  if defined? Gene and Entity === Gene
    module Gene
      property :genomes_1000_rsids => :single2array do
        Genomes1000.rsid_index(organism, chromosome)[self.chr_range]
      end

      property :genomes_1000_mutations => :single2array do
        GenomicMutation.setup(Genomes1000.mutation_index(organism).values_at(*self.genomes_1000_rsids).uniq, "1000 Genomes mutations over #{self.name || self}", organism, true)
      end
    end
  end

  if defined? GenomicMutation and Entity === GenomicMutation
    module GenomicMutation
      property :genomes_1000 => :array2single do
        Genomes1000.mutations.tsv(:persist => true, :key_field => "Genomic Mutation", :fields => ["Variant ID"], :type => :single).values_at *self
      end
    end
  end
end


