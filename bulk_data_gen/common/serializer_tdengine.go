package common

import (
	"io"
	"time"
)

var (
	database = []byte("benchmark_db")
)

type SerializerTDEngineSql struct {

}

func NewSerializerTDEngineSql() *SerializerTDEngineSql {
	return &SerializerTDEngineSql{}
}



// SerializePoint将Point数据写入指定的writer,并符合TDEngine的插入格式
//
// 此格式的输入如下所示:
// tag_name_list = tag_name_tag_name_tag_name
// INSERT INTO tag_name_list USING tablename TAGS(<tag_name list>) values (timestamp,<field values list>);
func (s *SerializerTDEngineSql) SerializePoint(w io.Writer, p *Point) (err error) {
	timestampNanos := p.Timestamp.Format(time.RFC3339Nano)
	buf := make([]byte, 0, 4096)
	//buf = append(buf, []byte("INSERT INTO ")...)
	//todo append tagnamelist

	buf = append(buf, database...)
	buf = append(buf, []byte(".t_")...)
	for i := 0; i < len(p.TagValues); i++ {
		buf = append(buf, p.TagValues[i]...)
		if i == len(p.TagValues) -1{
			continue
		}
		buf = append(buf, "_"...)
	}
	//buf = append(buf, p.MeasurementName...)
	buf = append(buf, []byte(" USING ")...)
	buf = append(buf, database...)
	buf = append(buf, []byte(".")...)
	buf = append(buf, p.MeasurementName...)

	buf = append(buf, []byte(" TAGS (")...)

	for i := 0; i < len(p.TagValues); i++ {
		buf = append(buf, p.TagValues[i]...)
		if i == len(p.TagValues) - 1{
			continue
		}
		buf = append(buf, ","...)

	}
	buf = append(buf, []byte(") VALUES ('")...)
	buf = append(buf, []byte(timestampNanos)...)
	buf = append(buf, []byte("'")...)
	for i := 0; i < len(p.FieldValues); i++ {
		buf = append(buf, ","...)
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, true)
	}
	buf = append(buf, []byte(")\n")...)
	if string(p.MeasurementName) == "window_state_room" {
		_, err = w.Write(buf)
	}
	if err != nil {
		return err
	}

	return nil
}

func (s *SerializerTDEngineSql) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}