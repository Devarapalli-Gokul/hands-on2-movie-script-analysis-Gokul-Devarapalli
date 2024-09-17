package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String dialogueLine[] = line.split(":");

        if(dialogueLine.length==2){

            String character = dialogueLine[0];
            String dialogue = dialogueLine[1];

            StringTokenizer tokenizer = new StringTokenizer(dialogue);

            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                characterWord.set(character+" : "+word);
                context.write(characterWord, one);
            }

        }


    }
}
