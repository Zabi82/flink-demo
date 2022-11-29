package com.flink.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.Arrays;
import java.util.List;

public class Utils {


    private static String[] WORD_LIST = new String[]{

    "word count from Wikipedia the free encyclopedia",
    "the word count is the number of words in a document or passage of text Word counting may be needed when a text",
    "is required to stay within certain numbers of words This may particularly be the case in academia legal",
    "proceedings journalism and advertising Word count is commonly used by translators to determine the price for",
    "the translation job Word counts may also be used to calculate measures of readability and to measure typing",
    "and reading speeds usually in words per minute When converting character counts to words a measure of five or",
    "six characters to a word is generally used Contents Details and variations of definition Software In fiction",
    "In non fiction See also References Sources External links Details and variations of definition"
    };

    public static List<String> getSampleWordList() {
        return Arrays.asList(WORD_LIST);
    }

    public static ObjectMapper getMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }
}
