package be.datablend.compoundcomparison.setup;

import fingerprinters.EncodingFingerprint;
import fingerprinters.features.FeatureMap;
import fingerprinters.features.IFeature;
import fingerprinters.topological.Encoding2DMolprint;
import io.reader.RandomAccessMDLReader;
import org.openscience.cdk.Molecule;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * User: dsuvee
 * Date: 27/01/13
 */
public class CompoundDatabase {

    private Jedis jedis = null;
    private ArrayList<String> fingerprintlist = new ArrayList<String>();

    public CompoundDatabase() throws UnknownHostException {
        // Init connection to redis
        jedis = new Jedis("localhost");
    }

    public void disconnect() {
        jedis.disconnect();
    }

    public Jedis getConnection() {
        return jedis;
    }


    public void create() throws IOException {
        // Import some compound data
        importCompounds();
    }

    private void importCompounds() throws IOException {
        // Create the compound file reader and fingerprinter
        System.out.println("Started reading the compound input file ... ");
        //RandomAccessMDLReader reader = new RandomAccessMDLReader(new File("/Users/dsuvee/Downloads/roadmap-2011-09-23-1.sdf"));
        RandomAccessMDLReader reader = new RandomAccessMDLReader(new File(getClass().getClassLoader().getResource("Compound_046200001_046225000.sdf").getFile()));
        EncodingFingerprint fingerprinter = new Encoding2DMolprint();
        System.out.println("Finished reading the compound input file ... ");

        // Start the import of the compounds
        System.out.println("Started import of " + reader.getSize() + " compounds  ... ");

        // We will use a pipeline in order to speedup the persisting process
        Pipeline p = jedis.pipelined();

        // Iterate the compounds one by one
        for (int i = 0; i < reader.getSize(); i++) {

            // Retrieve the molecule and the fingerprints for this molecule
            Molecule molecule = reader.getMol(i);
            FeatureMap fingerprints = new FeatureMap(fingerprinter.getFingerprint(molecule));

            // Retrieve some of the compound properties we want to use later on
            String compound_cid = (String)molecule.getProperty("PUBCHEM_COMPOUND_CID");

            // Iterate the fingerprints
            for (IFeature fingerprint : fingerprints.getKeySet()) {

                // Check whether we already encountered the feature and create accordingly (
                String thefeaturestring = fingerprint.featureToString();
                if (!fingerprintlist.contains(thefeaturestring)) {
                    fingerprintlist.add(thefeaturestring);
                }

                // Get the index of the fingerprint
                int fingerprintindex = fingerprintlist.indexOf(thefeaturestring);

                // Increment the weight of this fingerprint (number of occurences)
                p.incr(fingerprintindex + ":w");
                // Create the inverted indexes
                // Add the fingerprint to the set of fingerprints of this compound
                p.sadd((compound_cid + ":f"), fingerprintindex + "");
                // Add the compound to the set of compounds of this fingerprint
                p.sadd(fingerprintindex + ":c", compound_cid + "");

            }
            // Sync the changes
            p.sync();
        }

        System.out.println("Finished import of " + reader.getSize() + " compounds  ... \n");

    }

}