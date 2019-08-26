package com.grow;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Up2hdfs {
    private static String HDFSUrl = "hdfs://47.103.19.138:8020";

    public static void main(String[] args) throws Exception {
        FileSystem fs = getFileSystem();
        System.out.println(fs.getUsed());
        //创建路径
        mkdir("/ideaTest");
        //验证是否存在
//        System.out.println(existDir("/dit2", false));
//        //上传文件到HDFS
//        copyFileToHDFS("G:\\testFile\\HDFSTest.txt", "/dit/HDFSTest.txt");
//        //下载文件到本地
//        getFile("/dit/HDFSTest.txt", "G:\\HDFSTest.txt");
//        // getFile(HDFSFile,localFile);
//        //删除文件
//        rmdir("/dit2");
//        //读取文件
//        readFile("/dit/HDFSTest.txt");
    }

    public static FileSystem getFileSystem() {
        //读取配置文件
        Configuration conf = new Configuration();
        //文件系统
        FileSystem fs = null;
        String hdfsUrl = HDFSUrl;
        //返回默认文件系统，hadoop集群下使用
        if (StringUtils.isBlank(hdfsUrl)) {
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //本地测试使用
            URI uri = null;
            try {
                uri = new URI(hdfsUrl.trim());
                fs = FileSystem.get(uri, conf);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fs;
    }

    //创建文件目录
    public static void mkdir(String path) {
        FileSystem fs = getFileSystem();
        System.out.println("FilePath:" + path);
        try {
            fs.mkdirs(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断目录是否存在
    public static boolean existDir(String filePath, boolean create) {
        boolean flag = false;
        if (StringUtils.isNotEmpty(filePath)) {
            return flag;
        }

        try {
            Path path = new Path(filePath);
            FileSystem fs = getFileSystem();
            if (create) {
                if (!fs.exists(path)) {
                    fs.mkdirs(path);
                }
            }
            if (fs.exists(path)) {
                flag = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    //本地文件上传到HDFS
    public static void copyFileToHDFS(String srcFile, String destPath) {
        try {
            FileInputStream fis = new FileInputStream(new File(srcFile));//读取本地文件
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(HDFSUrl + destPath), config);
            OutputStream os = fs.create(new Path(destPath));
            IOUtils.copyBytes(fis, os, 4096, true);
            System.out.println("copy 完成 ......");
            fs.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //从HDFS下载文件到本地
    public static void getFile(String srcFile, String destPath) {
        //HDFS文件地址
        String file = HDFSUrl + srcFile;
        Configuration config = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(file), config);
            InputStream is = fs.open(new Path(file));
            IOUtils.copyBytes(is, new FileOutputStream(new File(destPath)), 2048, true);
            System.out.println("下载完成......");
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //读取文件
    }

    //删除文件/目录
    public static void rmdir(String path) {
        try {
            //返回FileSystem对象
            FileSystem fs = getFileSystem();

            String hdfsUri = HDFSUrl;
            if (StringUtils.isNotBlank(hdfsUri)) {

                path = hdfsUri + path;
            }
            System.out.println("path" + path);
            //删除文件或者文件目录 delete(Path f)此方法已经弃用
            System.out.println(fs.delete(new Path(path), true));

            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //读取文件内容到控制台
    public static void readFile(String filePath) throws IOException {
        Configuration config = new Configuration();
        String file = HDFSUrl + filePath;
        FileSystem fs = FileSystem.get(URI.create(file), config);
        //读取文件
        InputStream is = fs.open(new Path(file));
        //读取文件
        IOUtils.copyBytes(is, System.out, 2048, false); //复制到标准输出流
        fs.close();
    }

}
