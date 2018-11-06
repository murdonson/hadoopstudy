package com.yihao.beifengRec.offline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import com.yihao.beifengRec.common.ItemSimilarity;
import com.yihao.beifengRec.common.RedisUtil;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItem;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItems;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItemsWriter;
import redis.clients.jedis.Jedis;

// generate item-item similarity table
public class ItemsSimilarityTableRedisWriter implements SimilarItemsWriter {
  private long itemCounter = 0;
  private Jedis jedis = null;
  @Override
  public void open() throws IOException {
    jedis = RedisUtil.getJedis();
  }

  /**
   *  存储目标物品  与目标物品最相似的五个物品 与相似度
   * @param similarItems  目标物品
   * @throws IOException
     */
  @Override
  public void add(SimilarItems similarItems) throws IOException {
    ItemSimilarity[] values = new ItemSimilarity[similarItems.numSimilarItems()];
    int counter = 0;
    for (SimilarItem item: similarItems.getSimilarItems()) {
      values[counter] = new ItemSimilarity(item.getItemID(), item.getSimilarity());
      counter++;
    }
    String key = "II:" + similarItems.getItemID();
    String items = JSON.toJSONString(values);
    jedis.set(key, items);  // itemId, (simItemId,similarity)
    itemCounter++;
    if(itemCounter % 100 == 0) {
      System.out.println("Store " + key + " to redis, total:" + itemCounter);
    }
  }

  @Override
  public void close() throws IOException {
    jedis.close();
  }
}

