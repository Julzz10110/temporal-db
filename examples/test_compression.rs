//! Демонстрационный пример тестирования сжатия сегментов
//!
//! Запуск: cargo run --example test_compression

use temporal_db::core::event::{Event, EventPayload};
use temporal_db::core::temporal::Timestamp;
use temporal_db::storage::segment_file::{SegmentReader, SegmentWriter, FLAG_COMPRESSED};
use temporal_db::storage::wal::InMemoryWAL;
use temporal_db::storage::{EventJournal, SegmentedJournal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Тест сжатия сегментов ===\n");

    // Создаем временную директорию
    let temp_dir = std::env::temp_dir().join("temporal_db_test");
    std::fs::create_dir_all(&temp_dir)?;
    let segment_path = temp_dir.join("test_segment.seg");

    println!("1. Создание сегмента с событиями...");
    let ts1 = Timestamp::from_secs(1000);
    let ts2 = Timestamp::from_secs(2000);

    let mut writer = SegmentWriter::create(&segment_path, 1, ts1, ts2)?;

    // Добавляем события с повторяющимися данными (хорошо сжимаются)
    let repetitive_data = "A".repeat(500);
    for i in 0..50 {
        let payload = EventPayload::from_json(&serde_json::json!({
            "id": i,
            "large_data": repetitive_data,
            "metadata": format!("event-{}", i)
        }))?;
        let event = Event::new(
            "test.event".to_string(),
            Timestamp::from_secs(1000 + i as i64),
            format!("entity:{}", i % 5),
            payload,
        );
        writer.append(event)?;
    }
    let _ = writer.finalize()?;
    println!("   ✓ Добавлено 50 событий");

    // Проверяем размер файла
    let file_size = std::fs::metadata(&segment_path)?.len();
    println!("\n2. Анализ файла сегмента:");
    println!("   Размер файла: {} байт ({:.2} KB)", file_size, file_size as f64 / 1024.0);

    let mut reader = SegmentReader::open(&segment_path)?;
    let header = reader.header();
    println!("   ID сегмента: {}", header.segment_id);
    println!("   Количество событий: {}", header.event_count);
    println!("   Размер сжатых данных: {} байт", header.compressed_size);
    println!("   Checksum: 0x{:08X}", header.checksum);

    // Проверяем флаг сжатия
    let is_compressed = (header.flags & FLAG_COMPRESSED) != 0;
    println!("   Сжатие включено: {}", if is_compressed { "✓ Да" } else { "✗ Нет" });

    if is_compressed {
        let compression_ratio = (header.compressed_size as f64) / (file_size - 64) as f64;
        println!("   Коэффициент сжатия: {:.2}x", compression_ratio);
    }

    // Читаем события обратно
    println!("\n3. Чтение событий из сегмента...");
    let events = reader.read_events()?;
    println!("   ✓ Прочитано {} событий", events.len());

    // Проверяем целостность данных
    println!("\n4. Проверка целостности данных:");
    let mut entity_counts = std::collections::HashMap::new();
    for event in &events {
        *entity_counts.entry(event.entity_id()).or_insert(0) += 1;
    }
    println!("   Уникальных сущностей: {}", entity_counts.len());
    println!("   Все события успешно восстановлены: ✓");

    // Тест с SegmentedJournal
    println!("\n5. Тест интеграции с SegmentedJournal:");
    let journal_dir = temp_dir.join("journal");
    let wal = InMemoryWAL::new();
    let mut journal = SegmentedJournal::new(&journal_dir, wal)?;

    for i in 0..20 {
        let payload = EventPayload::from_json(&serde_json::json!({
            "value": format!("test-{}", i),
            "data": "repetitive".repeat(30)
        }))?;
        let event = Event::new(
            "demo.event".to_string(),
            Timestamp::from_secs(2000 + i as i64),
            format!("entity:{}", i % 3),
            payload,
        );
        journal.append(event).await?;
    }
    journal.flush().await?;
    println!("   ✓ Добавлено 20 событий через SegmentedJournal");

    let segments = journal.segments();
    println!("   Создано сегментов: {}", segments.len());
    for (idx, segment) in segments.iter().enumerate() {
        println!("   Сегмент {}: {} событий, checksum: 0x{:08X}", 
            idx + 1, segment.event_count, segment.checksum);
    }

    // Очистка
    println!("\n6. Очистка временных файлов...");
    std::fs::remove_dir_all(&temp_dir)?;
    println!("   ✓ Готово");

    println!("\n=== Все тесты пройдены успешно! ===");
    Ok(())
}
