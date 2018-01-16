/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.iflytek.GetHbaseScan;

import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * A HBase implementation of LoadFunc and StoreFunc.
 * <P>
 * Below is an example showing how to load data from HBase:
 * <pre>{@code
 * raw = LOAD 'hbase://SampleTable'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*', '-loadKey true -limit 5')
 *       AS (id:bytearray, first_name:chararray, last_name:chararray, friends_map:map[], info_map:map[]);
 * }</pre>
 * This example loads data redundantly from the info column family just to
 * illustrate usage. Note that the row key is inserted first in the result schema.
 * To load only column names that start with a given prefix, specify the column
 * name with a trailing '*'. For example passing <code>friends:bob_*</code> to
 * the constructor in the above example would cause only columns that start with
 * <i>bob_</i> to be loaded.
 * <P>
 * Note that when using a prefix like <code>friends:bob_*</code>, explicit HBase filters are set for
 * all columns and prefixes specified. Querying HBase with many filters can cause performance
 * degredation. This is typically seen when mixing one or more prefixed descriptors with a large list
 * of columns. In that case better perfomance will be seen by either loading the entire family via
 * <code>friends:*</code> or by specifying explicit column descriptor names.
 * <P>
 * Below is an example showing how to store data into HBase:
 * <pre>{@code
 * copy = STORE raw INTO 'hbase://SampleTableCopy'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*');
 * }</pre>
 * Note that STORE will expect the first value in the tuple to be the row key.
 * Scalars values need to map to an explicit column descriptor and maps need to
 * map to a column family name. In the above examples, the <code>friends</code>
 * column family data from <code>SampleTable</code> will be written to a
 * <code>buddies</code> column family in the <code>SampleTableCopy</code> table.
 *
 */
public class GetScan {

    private final static String ASTERISK = "*";
    private final static String COLON = ":";
    private final static String HBASE_SECURITY_CONF_KEY = "hbase.security.authentication";
    private final static String HBASE_CONFIG_SET = "hbase.config.set";
    private final static String HBASE_TOKEN_SET = "hbase.token.set";

    private List<ColumnInfo> columnInfo_ = Lists.newArrayList();
    private List<ColumnInfo> columnInfoRegex_ = Lists.newArrayList();

    //Use JobConf to store hbase delegation token
    private JobConf m_conf;
    private RecordReader reader;
    private RecordWriter writer;
    private TableOutputFormat outputFormat = null;
    private Scan scan;
    private String contextSignature = null;

    private final CommandLine configuredOptions_;
    private final static Options validOptions_ = new Options();
    private final static CommandLineParser parser_ = new GnuParser();
    private String rowRegx;

    private String delimiter_;
    private boolean ignoreWhitespace_;
    private final long limit_;
    private final int caching_;
    private final boolean noWAL_;
    private final long minTimestamp_;
    private final long maxTimestamp_;
    private final long timestamp_;

    protected transient byte[] gt_;
    protected transient byte[] gte_;
    protected transient byte[] lt_;
    protected transient byte[] lte_;

    private static void populateValidOptions() {
        validOptions_.addOption("gt", true, "Records must be greater than this value " +
                "(binary, double-slash-escaped)");
        validOptions_.addOption("lt", true, "Records must be less than this value (binary, double-slash-escaped)");
        validOptions_.addOption("gte", true, "Records must be greater than or equal to this value");
        validOptions_.addOption("lte", true, "Records must be less than or equal to this value");
        validOptions_.addOption("regex", true, "Records must match regex");
        validOptions_.addOption("caching", true, "Number of rows scanners should cache");
        validOptions_.addOption("limit", true, "Per-region limit");
        validOptions_.addOption("delim", true, "Column delimiter");
        validOptions_.addOption("ignoreWhitespace", true, "Ignore spaces when parsing columns");
        validOptions_.addOption("noWAL", false, "Sets the write ahead to false for faster loading. To be used with extreme caution since this could result in data loss (see http://hbase.apache.org/book.html#perf.hbase.client.putwal).");
        validOptions_.addOption("minTimestamp", true, "Record must have timestamp greater or equal to this value");
        validOptions_.addOption("maxTimestamp", true, "Record must have timestamp less then this value");
        validOptions_.addOption("timestamp", true, "Record must have timestamp equal to this value");

    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store the cells of the
     * provided columns.
     *
     * @param columnList
     *        columnlist that is a presented string delimited by space and/or
     *        commas. To retreive all columns in a column family <code>Foo</code>,
     *        specify a column as either <code>Foo:</code> or <code>Foo:*</code>.
     *        To fetch only columns in the CF that start with <I>bar</I>, specify
     *        <code>Foo:bar*</code>. The resulting tuple will always be the size
     *        of the number of tokens in <code>columnList</code>. Items in the
     *        tuple will be scalar values when a full column descriptor is
     *        specified, or a map of column descriptors to values when a column
     *        family is specified.
     *
     * @throws ParseException when unable to parse arguments
     * @throws IOException
     */

        /**
     * Replace sequences of two slashes ("\\") with one slash ("\")
     * (not escaping a slash in grunt is disallowed, but a double slash doesn't get converted
     * into a regular slash, so we have to do it instead)
     * @param str
     * @return the resulting string
     */
    public static String slashisize(String str) {
        return str.replace("\\\\", "\\");
    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store.
     * @param columnList
     * @param optString Loader options. Known options:<ul>
     * <li>-loadKey=(true|false)  Load the row key as the first column
     * <li>-gt=minKeyVal
     * <li>-lt=maxKeyVal
     * <li>-gte=minKeyVal
     * <li>-lte=maxKeyVal
     * <li>-limit=numRowsPerRegion max number of rows to retrieve per region
     * <li>-delim=char delimiter to use when parsing column names (default is space or comma)
     * <li>-ignoreWhitespace=(true|false) ignore spaces when parsing column names (default true)
     * <li>-caching=numRows  number of rows to cache (faster scans, more memory).
     * <li>-noWAL=(true|false) Sets the write ahead to false for faster loading.
     * <li>-minTimestamp= Scan's timestamp for min timeRange
     * <li>-maxTimestamp= Scan's timestamp for max timeRange
     * <li>-timestamp= Scan's specified timestamp
     * <li>-caster=(HBaseBinaryConverter|Utf8StorageConverter) Utf8StorageConverter is the default
     * To be used with extreme caution, since this could result in data loss
     * (see http://hbase.apache.org/book.html#perf.hbase.client.putwal).
     * </ul>
     * @throws ParseException
     * @throws IOException
     */
    public GetScan(String columnList, String optString) throws ParseException, IOException {
        populateValidOptions();
        String[] optsArr = optString.split(" ");
        try {
            configuredOptions_ = parser_.parse(validOptions_, optsArr);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "[-regex] [-gt] [-gte] [-lt] [-lte] [-columnPrefix] [-caching] [-caster] [-noWAL] [-limit] [-delim] [-ignoreWhitespace] [-minTimestamp] [-maxTimestamp] [-timestamp]", validOptions_ );
            throw e;
        }

        delimiter_ = ",";
        if (configuredOptions_.getOptionValue("delim") != null) {
          delimiter_ = configuredOptions_.getOptionValue("delim");
        }

        ignoreWhitespace_ = true;
        if (configuredOptions_.hasOption("ignoreWhitespace")) {
          String value = configuredOptions_.getOptionValue("ignoreWhitespace");
          if (!"true".equalsIgnoreCase(value)) {
            ignoreWhitespace_ = false;
          }
        }

        columnInfo_ = parseColumnList(columnList, delimiter_, ignoreWhitespace_).get(0);
        columnInfoRegex_ = parseColumnList(columnList, delimiter_, ignoreWhitespace_).get(1);

        caching_ = Integer.valueOf(configuredOptions_.getOptionValue("caching", "100"));
        limit_ = Long.valueOf(configuredOptions_.getOptionValue("limit", "-1"));
        noWAL_ = configuredOptions_.hasOption("noWAL");

        if (configuredOptions_.hasOption("minTimestamp")){
            minTimestamp_ = Long.parseLong(configuredOptions_.getOptionValue("minTimestamp"));
        } else {
            minTimestamp_ = 0;
        }

        if (configuredOptions_.hasOption("maxTimestamp")){
            maxTimestamp_ = Long.parseLong(configuredOptions_.getOptionValue("maxTimestamp"));
        } else {
            maxTimestamp_ = Long.MAX_VALUE;
        }

        if (configuredOptions_.hasOption("timestamp")){
            timestamp_ = Long.parseLong(configuredOptions_.getOptionValue("timestamp"));
        } else {
            timestamp_ = 0;
        }

        initScan();
    }

    /**
     *
     * @param columnList
     * @param delimiter
     * @param ignoreWhitespace
     * @return
     */
    private List<List<ColumnInfo>> parseColumnList(String columnList,
                                             String delimiter,
                                             boolean ignoreWhitespace) {
        List<List<ColumnInfo>> columnInfos = new ArrayList<List<ColumnInfo>>(2);
        List<ColumnInfo> columnInfo = new ArrayList<ColumnInfo>();
        List<ColumnInfo> columnInfoRegex = new ArrayList<ColumnInfo>();

        // Default behavior is to allow combinations of spaces and delimiter
        // which defaults to a comma. Setting to not ignore whitespace will
        // include the whitespace in the columns names
        String[] colNames = columnList.split(delimiter);
        if(ignoreWhitespace) {
            List<String> columns = new ArrayList<String>();

            for (String colName : colNames) {
                String[] subColNames = colName.split(" ");

                for (String subColName : subColNames) {
                    subColName = subColName.trim();
                    if (subColName.length() > 0) columns.add(subColName);
                }
            }

            colNames = columns.toArray(new String[columns.size()]);
        }

        for (String colName : colNames) {
            if(colName.contains(ASTERISK) && !colName.endsWith(ASTERISK) && (colName.indexOf(COLON) == colName.lastIndexOf(COLON)) && !colName.startsWith(COLON) && (colName.indexOf(COLON) < colName.indexOf(ASTERISK))){
                columnInfoRegex.add(new ColumnInfo(colName));
            }
            else {
                columnInfo.add(new ColumnInfo(colName));
            }
        }
        columnInfos.add(columnInfo);
        columnInfos.add(columnInfoRegex);
        return columnInfos;
    }

    private void initScan() throws IOException{
        scan = new Scan();

        // Map-reduce jobs should not run with cacheBlocks
        scan.setCacheBlocks(false);
        scan.setCaching(caching_);

        // Set filters, if any.
        if (configuredOptions_.hasOption("gt")) {
            gt_ = Bytes.toBytesBinary(slashisize(configuredOptions_.getOptionValue("gt")));
            addRowFilter(CompareOp.GREATER, gt_);
            scan.setStartRow(gt_);
        }
        if (configuredOptions_.hasOption("lt")) {
            lt_ = Bytes.toBytesBinary(slashisize(configuredOptions_.getOptionValue("lt")));
            addRowFilter(CompareOp.LESS, lt_);
            scan.setStopRow(lt_);
        }
        if (configuredOptions_.hasOption("gte")) {
            gte_ = Bytes.toBytesBinary(slashisize(configuredOptions_.getOptionValue("gte")));
            scan.setStartRow(gte_);
        }
        if (configuredOptions_.hasOption("lte")) {
            lte_ = Bytes.toBytesBinary(slashisize(configuredOptions_.getOptionValue("lte")));
            byte[] lt = increment(lte_);

            if (lt != null) {
                scan.setStopRow(increment(lte_));
            }

            // The WhileMatchFilter will short-circuit the scan after we no longer match. The
            // setStopRow call will limit the number of regions we need to scan
            addFilter(new WhileMatchFilter(new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(lte_))));
        }
        if(configuredOptions_.hasOption("regex")){
            rowRegx = configuredOptions_.getOptionValue("regex");
            if(rowRegx != null){
                addFilter(new RowFilter(CompareOp.EQUAL,new RegexStringComparator(rowRegx)));
            }
        }
        if (configuredOptions_.hasOption("minTimestamp") || configuredOptions_.hasOption("maxTimestamp")){
            scan.setTimeRange(minTimestamp_, maxTimestamp_);
        }
        if (configuredOptions_.hasOption("timestamp")){
            scan.setTimeStamp(timestamp_);
        }

        // if the group of columnInfos for this family doesn't contain a prefix, we don't need
        // to set any filters, we can just call addColumn or addFamily. See javadocs below.
        boolean columnPrefixExists = false;
        for (ColumnInfo columnInfo : columnInfo_) {
            if (columnInfo.getColumnPrefix() != null) {
                columnPrefixExists = true;
                break;
            }
        }

        if (!columnPrefixExists) {
            addFiltersWithoutColumnPrefix(columnInfo_);
        }
        else {
            addFiltersWithColumnPrefix(columnInfo_);
        }

        if(columnInfoRegex_.size() > 0){
            addFiltersWithColumnRegex(columnInfoRegex_);
        }
    }

    /**
     * If there is no column with a prefix, we don't need filters, we can just call addColumn and
     * addFamily on the scan
     */
    private void addFiltersWithoutColumnPrefix(List<ColumnInfo> columnInfos) {
        // Need to check for mixed types in a family, so we don't call addColumn 
        // after addFamily on the same family
        Map<String, List<ColumnInfo>> groupedMap = groupByFamily(columnInfos);
        for (Entry<String, List<ColumnInfo>> entrySet : groupedMap.entrySet()) {
            boolean onlyColumns = true;
            for (ColumnInfo columnInfo : entrySet.getValue()) {
                if (columnInfo.isColumnMap()) {
                    onlyColumns = false;
                    break;
                }
            }
            if (onlyColumns) {
                for (ColumnInfo columnInfo : entrySet.getValue()) {
                    scan.addColumn(columnInfo.getColumnFamily(), columnInfo.getColumnName());                    
                }
            } else {
                String family = entrySet.getKey();
                scan.addFamily(Bytes.toBytes(family));                
            }
        }
    }

    /**
     *  If we have a qualifier with a prefix and a wildcard (i.e. cf:foo*), we need a filter on every
     *  possible column to be returned as shown below. This will become very inneficient for long
     *  lists of columns mixed with a prefixed wildcard.
     *
     *  FilterList - must pass ALL of
     *   - FamilyFilter
     *   - AND a must pass ONE FilterList of
     *    - either Qualifier
     *    - or ColumnPrefixFilter
     *
     *  If we have only column family filters (i.e. cf:*) or explicit column descriptors
     *  (i.e., cf:foo) or a mix of both then we don't need filters, since the scan will take
     *  care of that.
     */
    private void addFiltersWithColumnPrefix(List<ColumnInfo> columnInfos) {
        // we need to apply a CF AND column list filter for each family
        FilterList allColumnFilters = null;
        Map<String, List<ColumnInfo>> groupedMap = groupByFamily(columnInfos);
        for (String cfString : groupedMap.keySet()) {
            List<ColumnInfo> columnInfoList = groupedMap.get(cfString);
            byte[] cf = Bytes.toBytes(cfString);

            // all filters roll up to one parent OR filter
            if (allColumnFilters == null) {
                allColumnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            }

            // each group contains a column family filter AND (all) and an OR (one of) of
            // the column filters
            FilterList thisColumnGroupFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            thisColumnGroupFilter.addFilter(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(cf)));
            FilterList columnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            for (ColumnInfo colInfo : columnInfoList) {
                if (colInfo.isColumnMap()) {

                    // add a PrefixFilter to the list of column filters
                    if (colInfo.getColumnPrefix() != null) {
                        columnFilters.addFilter(new ColumnPrefixFilter(
                            colInfo.getColumnPrefix()));
                    }
                }
                else {

                    // add a QualifierFilter to the list of column filters
                    columnFilters.addFilter(new QualifierFilter(CompareOp.EQUAL,
                            new BinaryComparator(colInfo.getColumnName())));
                }
            }
            thisColumnGroupFilter.addFilter(columnFilters);
            allColumnFilters.addFilter(thisColumnGroupFilter);
        }
        if (allColumnFilters != null) {
            addFilter(allColumnFilters);
        }
    }

    private void addFiltersWithColumnRegex(List<ColumnInfo> columnInfos) {
        // we need to apply a CF AND column list filter for each family
        FilterList allColumnFilters = null;
        Map<String, List<ColumnInfo>> groupedMap = groupByFamily(columnInfos);
        for (String cfString : groupedMap.keySet()) {
            List<ColumnInfo> columnInfoList = groupedMap.get(cfString);
            byte[] cf = Bytes.toBytes(cfString);

            // all filters roll up to one parent OR filter
            if (allColumnFilters == null) {
                allColumnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            }

            // each group contains a column family filter AND (all) and an OR (one of) of
            // the column filters
            FilterList thisColumnGroupFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            thisColumnGroupFilter.addFilter(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(cf)));
            FilterList columnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            for (ColumnInfo colInfo : columnInfoList) {
                columnFilters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(colInfo.getColumnRegex())));
            }
            thisColumnGroupFilter.addFilter(columnFilters);
            allColumnFilters.addFilter(thisColumnGroupFilter);
        }
        if (allColumnFilters != null) {
            addFilter(allColumnFilters);
        }
    }

    private void addRowFilter(CompareOp op, byte[] val) {

        addFilter(new RowFilter(op, new BinaryComparator(val)));
    }

    private void addFilter(Filter filter) {
        FilterList scanFilter = (FilterList) scan.getFilter();
        if (scanFilter == null) {
            scanFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        }
        scanFilter.addFilter(filter);
        scan.setFilter(scanFilter);
    }

   /**
    * Returns the ColumnInfo list so external objects can inspect it.
    * @return List of ColumnInfo objects
    */
    public List<ColumnInfo> getColumnInfoList() {
        return columnInfo_;
    }

   /**
    * Updates the ColumnInfo List. Use this if you need to implement custom projections
    */
    protected void setColumnInfoList(List<ColumnInfo> columnInfoList) {
        this.columnInfo_ = columnInfoList;
    }


    /**
     * Class to encapsulate logic around which column names were specified in each
     * position of the column list. Users can specify columns names in one of 4
     * ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result in a
     * Map being added to the tuple, while the last results in a scalar. The 3rd
     * form results in a prefix-filtered Map.
     */
    public class ColumnInfo {

        final String originalColumnName;  // always set
        String columnRegex;  // always set
        final byte[] columnFamily; // always set
        final byte[] columnName; // set if it exists and doesn't contain '*'
        final byte[] columnPrefix; // set if contains a prefix followed by '*'

        public ColumnInfo(String colName) {
            originalColumnName = colName;
            String[] cfAndColumn = colName.split(COLON, 2);

            //CFs are byte[1] and columns are byte[2]
            columnFamily = Bytes.toBytes(cfAndColumn[0]);
            if (cfAndColumn.length > 1 &&
                    cfAndColumn[1].length() > 0 && !ASTERISK.equals(cfAndColumn[1])) {
                if (cfAndColumn[1].endsWith(ASTERISK)) {
                    columnPrefix = Bytes.toBytes(cfAndColumn[1].substring(0,
                            cfAndColumn[1].length() - 1));
                    columnName = null;
                }
                else {
                    columnName = Bytes.toBytes(cfAndColumn[1]);
                    columnRegex = cfAndColumn[1];
                    columnPrefix = null;
                }
            } else {
              columnPrefix = null;
              columnName = null;
            }
        }

        public byte[] getColumnFamily() { return columnFamily; }
        public byte[] getColumnName() { return columnName; }
        public String getColumnRegex() { return columnRegex; }
        public byte[] getColumnPrefix() { return columnPrefix; }
        public boolean isColumnMap() { return columnName == null; }

        public boolean hasPrefixMatch(byte[] qualifier) {
            return Bytes.startsWith(qualifier, columnPrefix);
        }

        @Override
        public String toString() { return originalColumnName; }
    }

    /**
     * Group the list of ColumnInfo objects by their column family and returns a map of CF to its
     * list of ColumnInfo objects. Using String as key since it implements Comparable.
     * @param columnInfos the columnInfo list to group
     * @return a Map of lists, keyed by their column family.
     */
    static Map<String, List<ColumnInfo>> groupByFamily(List<ColumnInfo> columnInfos) {
        Map<String, List<ColumnInfo>> groupedMap = new HashMap<String, List<ColumnInfo>>();
        for (ColumnInfo columnInfo : columnInfos) {
            String cf = Bytes.toString(columnInfo.getColumnFamily());
            List<ColumnInfo> columnInfoList = groupedMap.get(cf);
            if (columnInfoList == null) {
                columnInfoList = new ArrayList<ColumnInfo>();
            }
            columnInfoList.add(columnInfo);
            groupedMap.put(cf, columnInfoList);
        }
        return groupedMap;
    }

    static String toString(byte[] bytes) {
        if (bytes == null) { return null; }

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) { sb.append("|"); }
            sb.append(bytes[i]);
        }
        return sb.toString();
    }

    /**
     * Increments the byte array by one for use with setting stopRow. If all bytes in the array are
     * set to the maximum byte value, then the original array will be returned with a 0 byte appended
     * to it. This is because HBase compares bytes from left to right. If byte array B is equal to
     * byte array A, but with an extra byte appended, A will be < B. For example
     * {@code}A = byte[] {-1}{@code} increments to
     * {@code}B = byte[] {-1, 0}{@code} and {@code}A < B{@code}
     * @param bytes array to increment bytes on
     * @return a copy of the byte array incremented by 1
     */
    static byte[] increment(byte[] bytes) {
        boolean allAtMax = true;
        for(int i = 0; i < bytes.length; i++) {
            if((bytes[bytes.length - i - 1] & 0x0ff) != 255) {
                allAtMax = false;
                break;
            }
        }

        if (allAtMax) {
            return Arrays.copyOf(bytes, bytes.length + 1);
        }

        byte[] incremented = bytes.clone();
        for(int i = bytes.length - 1; i >= 0; i--) {
            boolean carry = false;
            int val = bytes[i] & 0x0ff;
            int total = val + 1;
            if(total > 255) {
                carry = true;
                total %= 256;
            } else if (total < 0) {
                carry = true;
            }
            incremented[i] = (byte)total;
            if (!carry) return incremented;
        }
        return incremented;
    }

    public Scan getScan() {
        return scan;
    }
}
