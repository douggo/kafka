package ecommerce.model;

public class Email {

    private final String about;
    private final String subject;
    private final String body;

    public Email(String about, String subject, String body) {
        this.about = about;
        this.subject = subject;
        this.body = body;
    }

    public String getAbout() {
        return about;
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() {
        return body;
    }
}
