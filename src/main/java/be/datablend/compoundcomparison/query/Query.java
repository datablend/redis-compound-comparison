package be.datablend.compoundcomparison.query;

import be.datablend.compoundcomparison.setup.CompoundDatabase;
import redis.clients.jedis.*;
import java.io.IOException;
import java.util.*;

/**
 * User: dsuvee
 * Date: 27/01/13
 */
public class Query {

    private Jedis jedis = null;

    public Query(CompoundDatabase compoundDatabase) {
        // Get the connection to the redis database
        jedis = compoundDatabase.getConnection();
    }

    public void findSimilarCompounds(String compound, double similarity) throws IOException {

        long start = System.currentTimeMillis();

        String luaScript =
                // Retrieve the input paramters
                "local inputCompound = ARGV[1];" +
                "local similarity = ARGV[2];" +

                // Get the number of fingerprints of the input compound
                "local countToFind = redis.call('scard', inputCompound .. ':f');" +

                // Calculate the max, min and number of fingerprints to consider
                "local maxFingerprints = math.floor(countToFind / similarity);"+
                "local minFingerprints = math.floor(countToFind * similarity);"+
                "local numberOfFingerprintsToconsider = math.floor(countToFind - minFingerprints);"+

                // Retrieve the fingerprints of interest by subselecting out of the sorted list of fingerprints based upon the number of occurences
                "local fingerprintsOfInterest = redis.call('sort', inputCompound .. ':f', 'by', '*:w', 'limit', 0, numberOfFingerprintsToconsider + 1);\n" +

                // Create the set of all fingerprints we are interested in (creating the redis keys along the way)
                "local fingerprintKeys = {};"+
                "for index = 1, #fingerprintsOfInterest do " +
                        "table.insert(fingerprintKeys, fingerprintsOfInterest[index] .. ':c');"+
                "end "+

                // Retreive the set of possible matching compounds by taking the union of the fingerprint -> compound indexes
                "local possibleCompounds = redis.call('sunion',unpack(fingerprintKeys));"+

                // Calculate the tanimoto coefficient for the list of possible matching compounds with the inputcompound
                "local results = {};"+
                "for index = 1, #possibleCompounds do " +
                        // Check whether the count of fingerprints is within the predefined range
                        "local count = redis.call('scard', possibleCompounds[index] .. ':f');" +
                        "if count >= minFingerprints and count <= maxFingerprints then " +
                                // Calculate the matching fingerprints by calculating the intersection
                                "local intersectCount = redis.call('sinter', inputCompound .. ':f', possibleCompounds[index] .. ':f');" +
                                "local tanimoto = #intersectCount / (count + countToFind - #intersectCount);" +
                                // If sufficient similar, add it the set of results
                                "if tanimoto >= tonumber(similarity) then "+
                                        "table.insert(results,possibleCompounds[index]); table.insert(results, '' .. tanimoto);"+
                                "end "+
                        "end "+
                "end "+

                // Return the results
                "return results;";

        Object t = jedis.eval(luaScript, 0, compound, similarity+"");

        System.out.println("Found " + ((ArrayList)t).size()/2 + " matching compounds in " + (System.currentTimeMillis()-start) + " ms");

    }

}
