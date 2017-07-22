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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

public class TestUID {
  
  @Test 
  public void TestgenerateNAMTfromEvent() {
    Long ts = (long) 1200000000;
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "testHost");
    String[] s = {"someMetric"};
    MetaEvent event = new MetaEvent("testNamespace", "someapp", s, ts, ts, tags, 1, false);
    String namt = UID.generateNAMTfromEvent(event);
    Assert.assertEquals("testNamespace.someapp.someMetric.host=testHost", namt);
  }

  @Test (expected = NullPointerException.class)
  public void TestgenerateNAMTfromEventwithNull() {
    Long ts = (long) 1200000000;
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", null);
    String[] s = {"someMetric"};
    MetaEvent event = new MetaEvent("testNamespace", "someapp", s, ts, ts, tags, 1, false);
    String namt = UID.generateNAMTfromEvent(event);
    Assert.assertEquals("testNamespace.someapp.someMetric.host=testHost", namt);
  }

}
