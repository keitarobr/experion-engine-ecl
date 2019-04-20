package br.ufsc.ppgcc.experion.extractor.input.engine.ecl;

import br.ufsc.ppgcc.experion.extractor.input.engine.technique.KeygraphExtractionTechniqueTFIDF;

public class KeygraphExtractionTechniqueECLTFIDF extends KeygraphExtractionTechniqueTFIDF {

    public KeygraphExtractionTechniqueECLTFIDF() {
        super(new KeyGraphECL());
    }

}
