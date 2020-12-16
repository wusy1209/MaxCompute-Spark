package com.aliyun.odps.spark.examples.models;

public class ItemJeyooTopic extends ItemBase {
    private String question;
    private String knowledge;
    private String answer;

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getKnowledge() {
        return knowledge;
    }

    public void setKnowledge(String knowledge) {
        this.knowledge = knowledge;
    }

    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    public void setFathor(ItemBase itemBase){
        this.setId(itemBase.getId());
        this.setUrl(itemBase.getUrl());
        this.setCrawer_time(itemBase.getCrawer_time());
        this.setVersion(itemBase.getVersion());
        this.setPp(itemBase.getPp());
    }
}
