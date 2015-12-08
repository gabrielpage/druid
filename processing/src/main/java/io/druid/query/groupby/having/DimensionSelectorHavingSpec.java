/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.metamx.common.StringUtils;
import io.druid.data.input.Row;

import java.nio.ByteBuffer;
import java.util.List;

public class DimensionSelectorHavingSpec implements HavingSpec
{
  private static final byte CACHE_KEY = (byte) 0x8;
  private static final byte STRING_SEPARATOR = (byte) 0xFF;
  private final String dimension;
  private final String value;

  @JsonCreator
  public DimensionSelectorHavingSpec(
      @JsonProperty("dimension") String dimName,
      @JsonProperty("value") String value
  )
  {
    this.dimension = Preconditions.checkNotNull(dimName, "Must have attribute 'dimension'");
    this.value = value;
  }

  @JsonProperty("value")
  public String getValue()
  {
    return this.value;
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return this.dimension;
  }

  public boolean eval(Row row)
  {
    List<String> dimRowValList = row.getDimension(this.dimension);
    if (dimRowValList == null || dimRowValList.isEmpty()) {
      return Strings.isNullOrEmpty(value);
    }

    for (String rowVal : dimRowValList) {
      if (this.value != null && this.value.equals(rowVal)) {
        return true;
      }
      if (rowVal == null || rowVal.isEmpty()) {
        return Strings.isNullOrEmpty(value);
      }
    }

    return false;
  }

  public byte[] getCacheKey()
  {
    byte[] dimBytes = StringUtils.toUtf8(this.dimension);
    byte[] valBytes = StringUtils.toUtf8(this.value);
    return ByteBuffer.allocate(2 + dimBytes.length + valBytes.length)
                     .put(CACHE_KEY)
                     .put(dimBytes)
                     .put(STRING_SEPARATOR)
                     .put(valBytes)
                     .array();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionSelectorHavingSpec that = (DimensionSelectorHavingSpec) o;
    boolean valEquals = false;
    boolean dimEquals = false;

    if (value != null && that.value != null) {
      valEquals = value.equals(that.value);
    } else if (value == null && that.value == null) {
      valEquals = true;
    }

    if (dimension != null && that.dimension != null) {
      dimEquals = dimension.equals(that.dimension);
    } else if (dimension == null && that.dimension == null) {
      dimEquals = true;
    }

    return (valEquals && dimEquals);
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }


  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("DimensionSelectorHavingSpec");
    sb.append("{dimension='").append(this.dimension).append('\'');
    sb.append(", value='").append(this.value).append('\'');
    sb.append('}');
    return sb.toString();
  }

}
