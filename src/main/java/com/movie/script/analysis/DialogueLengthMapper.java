package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String dialogueLine[] = line.split(":");

        if(dialogueLine.length==2){
            StringTokenizer tokenizer = new StringTokenizer(dialogueLine[1]);
            character.set(dialogueLine[0]);
            wordCount.set(tokenizer.countTokens());
            context.write(character, wordCount);
        }

        
    }
}
