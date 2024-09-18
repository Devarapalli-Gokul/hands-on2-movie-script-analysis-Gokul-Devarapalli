package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String dialogueLine[] = line.split(":");
        
        if(dialogueLine.length==2){
            character.set(dialogueLine[0]);
            StringTokenizer tokenizer = new StringTokenizer(dialogueLine[1]);
            HashSet<String> uniqueWords = new HashSet<>();
            while(tokenizer.hasMoreTokens()){
                String checkToken = tokenizer.nextToken();
                if(!uniqueWords.contains(checkToken)){
                    uniqueWords.add(checkToken);
                    word.set(checkToken);
                    context.write(character, word);
                }
            }
        }
    }
}
