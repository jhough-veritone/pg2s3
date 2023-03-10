#[derive(Debug, Clone)]
pub struct ColumnData {
    pub name: String,
    pub data_type: String,
    pub formatted_name: String,
    pub current_value: String,
}