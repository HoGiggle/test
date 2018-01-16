package com.hust;
/**
 *
 * @author llxiang
 * @date 2015-02-03
 * @function vad
 *
 */
public class JniSegment {

    public final static int Segment_RET_SUCCESS		=	0;
    public final static long Segment_INVALID_HANDLER	=	0;

    static {
        String libName = "segmentltp";
        try {
            System.loadLibrary(libName);
            System.out.println("load lib: " + libName);
        } catch (Exception e) {
            System.err.println("error loading lib: " + e.toString());
        }
    }

    //declare functions in jni

    //only once in the hole process, now the resource is the modes path
    public native static long create(String resPath);

    //
    public native static String segmentNews(long handler, String newsContent);
    //
    public native static String segmentNewsInSentence(long handler, String newsContent);
    //
    public native static String segmentNews(long handler, byte[] newsContent);
    //
    public native static String segmentNewsInSentence(long handler, byte[] newsContent);
    //
    public native static int segmentNewsFile(long handler, String newsFile, String resultFile);

    //destroy correspond the create
    public native static int destroy(long handler);
}