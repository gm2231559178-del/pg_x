use anyhow::{Context, Result};
use rust_xlsxwriter::*;
use std::path::Path;

use crate::utils::format::RowSet;

/// Excel hard limits (.xlsx)
const EXCEL_MAX_ROWS: usize = 1_048_576;
const EXCEL_MAX_COLS: usize = 16_384;
const HEADER_ROWS: usize = 1;
const DATA_ROWS_PER_SHEET: usize = EXCEL_MAX_ROWS - HEADER_ROWS;

/// Sanitize and clamp Excel worksheet names
fn sanitize_sheet_name(name: &str) -> String {
    let invalid = [':', '\\', '/', '?', '*', '[', ']'];
    let mut clean: String = name.chars().filter(|c| !invalid.contains(c)).collect();

    if clean.len() > 31 {
        clean.truncate(31);
    }

    if clean.is_empty() {
        clean.push_str("Sheet");
    }

    clean
}

/// Write a `RowSet` to an Excel file with rich formatting.
/// Automatically splits into multiple worksheets if limits are exceeded.
pub fn write_excel(
    rowset: &RowSet,
    path: &Path,
    sheet_name: &str,
    freeze_header: bool,
    autofit: bool,
    stripe: bool,
) -> Result<()> {
    // ─────────────────────────────────────────────────────────────────────────
    // Safety checks
    // ─────────────────────────────────────────────────────────────────────────
    if rowset.columns.len() > EXCEL_MAX_COLS {
        anyhow::bail!(
            "Too many columns: {} (Excel limit: {})",
            rowset.columns.len(),
            EXCEL_MAX_COLS
        );
    }

    let mut wb = Workbook::new();
    let base_sheet_name = sanitize_sheet_name(sheet_name);

    let total_rows = rowset.rows.len();
    let sheet_count = total_rows.div_ceil(DATA_ROWS_PER_SHEET);

    // ─────────────────────────────────────────────────────────────────────────
    // Formats
    // ─────────────────────────────────────────────────────────────────────────
    let fmt_header = Format::new()
        .set_bold()
        .set_font_color(Color::White)
        .set_background_color(Color::RGB(0x2E_74_B5))
        .set_border_bottom(FormatBorder::Medium)
        .set_text_wrap();

    let fmt_even = Format::new().set_background_color(Color::RGB(0xDD_EA_F1));
    let fmt_odd = Format::new();

    // ─────────────────────────────────────────────────────────────────────────
    // Sheets
    // ─────────────────────────────────────────────────────────────────────────
    for sheet_idx in 0..sheet_count {
        let ws = wb.add_worksheet();

        let name = if sheet_idx == 0 {
            base_sheet_name.clone()
        } else {
            format!("{} ({})", base_sheet_name, sheet_idx + 1)
        };

        ws.set_name(&name).context("Invalid sheet name")?;

        // ---- Header ---------------------------------------------------------
        for (col, column_name) in rowset.columns.iter().enumerate() {
            ws.write_with_format(0, col as u16, column_name, &fmt_header)
                .context("Write header cell")?;
        }
        ws.set_row_height(0, 22.0).ok();

        // Track column widths per sheet
        let mut col_widths: Vec<f64> = rowset
            .columns
            .iter()
            .map(|c| c.chars().count() as f64)
            .collect();

        // ---- Data -----------------------------------------------------------
        let start = sheet_idx * DATA_ROWS_PER_SHEET;
        let end = (start + DATA_ROWS_PER_SHEET).min(total_rows);

        for (local_row_idx, row) in rowset.rows[start..end].iter().enumerate() {
            let excel_row = (local_row_idx + 1) as u32;

            let row_fmt = if stripe && local_row_idx % 2 == 1 {
                Some(&fmt_even)
            } else {
                Some(&fmt_odd)
            };

            for (col_idx, cell) in row.iter().enumerate().take(EXCEL_MAX_COLS) {
                let col = col_idx as u16;

                // Track column width
                let len = cell.chars().count() as f64;
                if len > col_widths[col_idx] {
                    col_widths[col_idx] = len;
                }

                if cell == "\0NULL" {
                    // SQL NULL — write blank cell
                    ws.write(excel_row, col, "")?;
                } else if let Ok(n) = cell.parse::<f64>() {
                    if let Some(fmt) = row_fmt {
                        ws.write_number_with_format(excel_row, col, n, fmt)?;
                    } else {
                        ws.write_number(excel_row, col, n)?;
                    }
                } else if cell == "true" || cell == "false" {
                    let b = cell == "true";
                    if let Some(fmt) = row_fmt {
                        ws.write_boolean_with_format(excel_row, col, b, fmt)?;
                    } else {
                        ws.write_boolean(excel_row, col, b)?;
                    }
                } else {
                    if let Some(fmt) = row_fmt {
                        ws.write_with_format(excel_row, col, cell.as_str(), fmt)?;
                    } else {
                        ws.write(excel_row, col, cell.as_str())?;
                    }
                }
            }
        }

        // ---- Autofit --------------------------------------------------------
        if autofit {
            for (col_idx, &width) in col_widths.iter().enumerate() {
                let w = width.clamp(6.0, 60.0) + 2.0;
                ws.set_column_width(col_idx as u16, w)?;
            }
        }

        // ---- Freeze header --------------------------------------------------
        if freeze_header {
            ws.set_freeze_panes(1, 0)?;
        }

        // ---- Autofilter -----------------------------------------------------
        if !rowset.columns.is_empty() {
            ws.autofilter(
                0,
                0,
                (end - start) as u32,
                (rowset.columns.len() - 1) as u16,
            )?;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Save
    // ─────────────────────────────────────────────────────────────────────────
    wb.save(path)
        .with_context(|| format!("Cannot save Excel file: {}", path.display()))?;

    Ok(())
}
