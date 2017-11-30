package sopra.processors.FlatText;

public class Column {
    private int index;
    private int length;
    private String name;

    public Column(String name, int index, int length) {
        this.index = index;
        this.length = length;
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public int getLength() {
        return length;
    }

    public String getName() {
        return name;
    }
}

