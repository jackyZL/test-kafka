package com.zl.partion;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by jacky on 2017/2/21.
 */
public class SimplePartitioner implements Partitioner {

    public SimplePartitioner (VerifiableProperties props) {

    }

    @Override
    public int partition(Object key, int a_numPartitions) {

        //System.out.println("partions"+a_numPartitions);

        int partition = 0;
        String stringKey = (String) key;

        //System.out.println("key:"+stringKey);

        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
        System.out.println("send to partition:" + partition);
        return partition;
    }
}
