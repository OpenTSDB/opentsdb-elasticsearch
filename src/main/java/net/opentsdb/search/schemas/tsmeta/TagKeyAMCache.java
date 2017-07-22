// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.search.schemas.tsmeta;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import gnu.trove.set.hash.TCustomHashSet;
import gnu.trove.set.hash.TLongHashSet;

public class TagKeyAMCache {
  
  public static TLongHashSet tagkeyCache = new TLongHashSet();
  public static TLongHashSet amCache = new TLongHashSet();

  public TagKeyAMCache() {
    TagKeyAmCacheCleaner cachePurge = new TagKeyAmCacheCleaner();
    Timer timer = new Timer();
    timer.schedule(cachePurge, 0, 24 * 60 * 60 * 1000);
  }
  
  
  public TLongHashSet getTagkeyCache() {
    return tagkeyCache;
  }
  public void setTagkeyCache(TLongHashSet tagkeyCache) {
    this.tagkeyCache = tagkeyCache;
  }
  public TLongHashSet getAmCache() {
    return amCache;
  }
  public void setAmCache(TLongHashSet amCache) {
    this.amCache = amCache;
  }
  
  private class TagKeyAmCacheCleaner extends TimerTask {
    AtomicInteger thresholdCounter;
    public TagKeyAmCacheCleaner() {
      tagkeyCache.clear();
      amCache.clear();
    }
    
    @Override
    public void run() {  
      synchronized (thresholdCounter) {
        thresholdCounter.set(0);
        thresholdCounter.notifyAll();
      }

    }

  }

}
