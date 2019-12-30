package jungfly.kda.task;

public class RawEvent {
    private String type;
    private String id;
    private String op;
    private byte[] smile;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public byte[] getSmile() {
        return smile;
    }

    public void setSmile(byte[] smile) {
        this.smile = smile;
    }
}
