#ifndef __NIRTREE__
#define __NIRTREE__

#include <cassert>
#include <vector>
#include <stack>
#include <queue>
#include <string>
#include <iostream>
#include <utility>
#include <util/geometry.h>
#include <nirtree/node.h>
#include <index/index.h>
#include <util/pencilPrinter.h>

namespace nirtree
{
	// TODO: For now we will work with a NIRTree that only stores points
	class NIRTree: public Index
	{
		public:
			Node *root;

			// Constructors and destructors
			NIRTree(unsigned minBranchFactor, unsigned maxBranchFactor);
			NIRTree(Node *root);
			~NIRTree();

			// Datastructure interface
			std::vector<Point> exhaustiveSearch(Point requestedPoint);
			std::vector<Point> search(Point requestedPoint);
			std::vector<Point> search(Rectangle requestedRectangle);
			void insert(Point givenPoint);
			void remove(Point givenPoint);

			// Miscellaneous
			unsigned checksum();
			void stat();
			void print();
			void pencilPrint();
	};
}

#endif
