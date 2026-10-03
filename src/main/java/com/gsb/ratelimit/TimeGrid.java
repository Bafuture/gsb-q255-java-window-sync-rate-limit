package com.gsb.ratelimit;

/**
 * 全局时间格：把时间轴切成固定长度的小格（cell），
 * 每 cellsPerWindow 个连续小格构成一个窗口（window）。
 * 所有节点与中心必须使用相同参数的 TimeGrid，窗口边界由中心时间基准决定。
 */
public final class TimeGrid {

    private final long cellMillis;
    private final int cellsPerWindow;

    public TimeGrid(long cellMillis, int cellsPerWindow) {
        if (cellMillis <= 0) {
            throw new IllegalArgumentException("cellMillis must be > 0");
        }
        if (cellsPerWindow <= 0) {
            throw new IllegalArgumentException("cellsPerWindow must be > 0");
        }
        this.cellMillis = cellMillis;
        this.cellsPerWindow = cellsPerWindow;
    }

    public long cellMillis() {
        return cellMillis;
    }

    public int cellsPerWindow() {
        return cellsPerWindow;
    }

    public long windowMillis() {
        return cellMillis * cellsPerWindow;
    }

    /** 时间戳所属的小格编号。 */
    public long cellIndex(long timeMillis) {
        return Math.floorDiv(timeMillis, cellMillis);
    }

    /** 小格所属的窗口编号。 */
    public long windowIndexOfCell(long cellIndex) {
        return Math.floorDiv(cellIndex, cellsPerWindow);
    }

    /** 时间戳所属的窗口编号。 */
    public long windowIndex(long timeMillis) {
        return windowIndexOfCell(cellIndex(timeMillis));
    }

    /** 窗口的第一个小格编号。 */
    public long firstCellOfWindow(long windowIndex) {
        return windowIndex * cellsPerWindow;
    }
}
