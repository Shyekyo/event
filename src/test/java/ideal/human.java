package ideal;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by zhangxiaofan on 2019/8/8.
 */
public class human implements Serializable {
    private String user;
    private Date postDate;
    private String message;

    public human() {
    }

    public human(String user, Date postDate, String message) {
        this.user = user;
        this.postDate = postDate;
        this.message = message;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Date getPostDate() {
        return postDate;
    }

    public void setPostDate(Date postDate) {
        this.postDate = postDate;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
