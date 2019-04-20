package br.ufsc.ppgcc.experion.extractor.input.engine.ecl;

import br.ufsc.ppgcc.experion.extractor.algorithm.keygraph.Keygraph;

import java.io.InputStream;
import java.net.URL;

public class KeyGraphECL extends Keygraph {
    @Override
    public InputStream getConfig() {
        return this.getClass().getResourceAsStream("/ECLConstants.txt");
    }
}
