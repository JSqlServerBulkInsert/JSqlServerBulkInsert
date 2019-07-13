package de.bytefish.jsqlserverbulkinsert.converters;

public class CharacterConverter extends BaseConverter<Character> {
    @Override
    public Object internalConvert(Character value) {
        return value;
    }
}
