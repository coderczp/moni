package org.virtualdb.mpp;

import java.util.Comparator;

public interface MemMapSorter {

	MemMapSorter HEAP_SORTER = new HeapSorter();
	MemMapSorter MERGE_SORTER = new MergeSorter();

	void sort(MemMapByteArray arr, Comparator<byte[]> cmp);

	class HeapSorter implements MemMapSorter {

		public void sort(MemMapByteArray arr, Comparator<byte[]> cmp) {
			buildHeap(arr, cmp);
			int n = arr.size();
			for (int i = n - 1; i >= 1; i--) {
				arr.swap(0, i);
				heapify(arr, 0, i, cmp);
			}
		}

		private void buildHeap(MemMapByteArray arr, Comparator<byte[]> cmp) {
			int n = arr.size();
			for (int i = n / 2 - 1; i >= 0; i--)
				heapify(arr, i, n, cmp);
		}

		private void heapify(MemMapByteArray arr, int idx, int max,
				Comparator<byte[]> cmp) {
			int index = 2 * idx;
			int left = index + 1;// 左孩子的下标（如果存在的话）
			int right = index + 2;// 左孩子的下标（如果存在的话）
			int largest = 0;// 寻找3个节点中最大值节点的下标

			if (left < max && cmp.compare(arr.get(left), arr.get(idx)) > 0)
				largest = left;
			else
				largest = idx;
			if (right < max
					&& cmp.compare(arr.get(right), arr.get(largest)) > 0)
				largest = right;
			if (largest != idx) {
				arr.swap(largest, idx);
				heapify(arr, largest, max, cmp);
			}

		}
	}

	class MergeSorter implements MemMapSorter {

		public void sort(MemMapByteArray arr, Comparator<byte[]> cmp) {
			mergeSort(arr, 0, arr.size() - 1, cmp);
		}

		private void reverse(MemMapByteArray arr, int left, int size) {
			int right = size - 1;
			while (left < right) {
				arr.swap(left++, right--);
			}
		}

		/***
		 * 必须保证[start,mid](mid,right]<br/>
		 * 是有序的 三次翻转后[start,right]就是有数数组
		 * 
		 * @param arr
		 * @param start
		 * @param mid
		 * @param right
		 */
		private void swap(MemMapByteArray arr, int start, int mid, int right) {
			reverse(arr, start, mid);/* 先反转左手 */
			reverse(arr, mid, right);/* 再反转右手 */
			reverse(arr, start, right);/* 两个手一起反转 */
		}

		private void merge(MemMapByteArray arr, int beg, int mid, int end,
				Comparator<byte[]> cmp) {
			int i = beg; // 指示有序串1
			int j = mid + 1; // 指示有序串2
			while (i < j && j <= end) {
				byte[] jValue = arr.get(j);
				while (i < j && cmp.compare(arr.get(i), jValue) <= 0)
					++i;
				int index = j;
				byte[] iValue = arr.get(i);
				while (j <= end && cmp.compare(arr.get(j), iValue) <= 0)
					++j;
				// swap [i,index) and [index,j)
				swap(arr, i, index, j);
				i += (j - index);
			}
		}

		private void mergeSort(MemMapByteArray arr, int beg, int end,
				Comparator<byte[]> cmp) {
			if (beg < end) {
				int mid = (beg + end) / 2;
				mergeSort(arr, beg, mid, cmp);
				mergeSort(arr, mid + 1, end, cmp);
				merge(arr, beg, mid, end, cmp);
			}
		}

	}

}